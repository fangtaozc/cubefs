// Copyright 2026 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package master

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

// Master raft persistence for the M3 oss-accel eviction rule — one
// water-level schedule per volume. Byte-for-byte the same pattern as
// oss_accel_changelog_rule_store.go (which itself mirrors the lifecycle
// config pattern); see that file's header comment for the rationale.

const (
	ossAccelEvictionRuleAcronym = "oaer"
	ossAccelEvictionRulePrefix  = keySeparator + ossAccelEvictionRuleAcronym + keySeparator
)

// OSSAccelEvictionRuleCache holds the master's in-memory view of every
// persisted per-volume eviction rule, keyed by volume name.
type OSSAccelEvictionRuleCache struct {
	mu    sync.RWMutex
	rules map[string]*proto.OSSAccelEvictionRule // volName -> rule

	// updateMu 序列化"读现有规则→合并派发态字段(CreatedAt/LastRunAt/
	// LastRunResult/EvictionInFlight)→写回"这类复合操作。mu本身只保护map的
	// 单次Get/Put/Delete,不能覆盖跨越Get+Put两步的read-modify-write——HTTP
	// setOSSAccelEvictionRule handler跟后台tick()/fireRule各自都是这种复合
	// 操作,中间没有共享锁时会互相踩踏(TOCTOU:后写的覆盖丢失前一个的更新,
	// 例如把刚被tick()置true的EvictionInFlight又拍回旧值)。调用方需要在
	// Get和Put之间持有这把锁,见LockUpdate/UnlockUpdate。
	updateMu sync.Mutex
}

// LockUpdate/UnlockUpdate serialize the Get-then-modify-then-Put sequences
// used by the HTTP Set handler and the manager's tick()/fireRule — see
// updateMu's doc comment for why mu alone isn't enough.
func (c *OSSAccelEvictionRuleCache) LockUpdate()   { c.updateMu.Lock() }
func (c *OSSAccelEvictionRuleCache) UnlockUpdate() { c.updateMu.Unlock() }

func NewOSSAccelEvictionRuleCache() *OSSAccelEvictionRuleCache {
	return &OSSAccelEvictionRuleCache{rules: make(map[string]*proto.OSSAccelEvictionRule)}
}

func (c *OSSAccelEvictionRuleCache) Get(volName string) *proto.OSSAccelEvictionRule {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.rules[volName]
}

func (c *OSSAccelEvictionRuleCache) List() []*proto.OSSAccelEvictionRule {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]*proto.OSSAccelEvictionRule, 0, len(c.rules))
	for _, r := range c.rules {
		out = append(out, r)
	}
	return out
}

func (c *OSSAccelEvictionRuleCache) Put(r *proto.OSSAccelEvictionRule) {
	if c == nil || r == nil {
		return
	}
	c.mu.Lock()
	c.rules[r.VolName] = r
	c.mu.Unlock()
}

func (c *OSSAccelEvictionRuleCache) Delete(volName string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	delete(c.rules, volName)
	c.mu.Unlock()
}

func (c *Cluster) syncAddOSSAccelEvictionRule(r *proto.OSSAccelEvictionRule) error {
	return c.syncPutOSSAccelEvictionRuleInfo(opSyncAddOSSAccelEvictionRule, r)
}

func (c *Cluster) syncUpdateOSSAccelEvictionRule(r *proto.OSSAccelEvictionRule) error {
	return c.syncPutOSSAccelEvictionRuleInfo(opSyncUpdateOSSAccelEvictionRule, r)
}

func (c *Cluster) syncDeleteOSSAccelEvictionRule(r *proto.OSSAccelEvictionRule) error {
	return c.syncPutOSSAccelEvictionRuleInfo(opSyncDeleteOSSAccelEvictionRule, r)
}

func (c *Cluster) syncPutOSSAccelEvictionRuleInfo(opType uint32, r *proto.OSSAccelEvictionRule) error {
	if r == nil || r.VolName == "" {
		return fmt.Errorf("syncPutOSSAccelEvictionRuleInfo: nil rule or empty volName")
	}
	metadata := new(RaftCmd)
	metadata.Op = opType
	metadata.K = ossAccelEvictionRulePrefix + r.VolName
	var err error
	metadata.V, err = json.Marshal(r)
	if err != nil {
		return fmt.Errorf("syncPutOSSAccelEvictionRuleInfo marshal vol=%s: %w", r.VolName, err)
	}
	return c.submit(metadata)
}

// loadOSSAccelEvictionRules reconstructs the in-memory cache from the raft
// store on master start (cold load) and on leader switch.
func (c *Cluster) loadOSSAccelEvictionRules() (err error) {
	if c.ossAccelEvictionRuleCache == nil {
		c.ossAccelEvictionRuleCache = NewOSSAccelEvictionRuleCache()
	}
	result, err := c.fsm.store.SeekForPrefix([]byte(ossAccelEvictionRulePrefix))
	if err != nil {
		return fmt.Errorf("action[loadOSSAccelEvictionRules],err:%v", err.Error())
	}
	loaded := 0
	for k, value := range result {
		rule := &proto.OSSAccelEvictionRule{}
		if err = json.Unmarshal(value, rule); err != nil {
			log.LogErrorf("action[loadOSSAccelEvictionRules],key:%s unmarshal err:%v", k, err)
			err = nil
			continue
		}
		if rule.VolName == "" {
			log.LogWarnf("action[loadOSSAccelEvictionRules],key:%s rule has empty volName, skipping", k)
			continue
		}
		// EvictionInFlight is dispatch-in-progress bookkeeping, not durable
		// truth — a rule loaded fresh from raft (cold start / leader switch)
		// can never have a task actually in flight against THIS process, so
		// clear it defensively rather than trusting whatever was persisted
		// the last time this rule's record was written.
		rule.EvictionInFlight = false
		c.ossAccelEvictionRuleCache.Put(rule)
		loaded++
	}
	log.LogInfof("action[loadOSSAccelEvictionRules], loaded %d of %d records", loaded, len(result))
	return
}

// SetOSSAccelEvictionRule creates or replaces the volume's eviction schedule.
func (c *Cluster) SetOSSAccelEvictionRule(r *proto.OSSAccelEvictionRule) error {
	var err error
	if c.ossAccelEvictionRuleCache.Get(r.VolName) != nil {
		err = c.syncUpdateOSSAccelEvictionRule(r)
	} else {
		err = c.syncAddOSSAccelEvictionRule(r)
	}
	if err != nil {
		return fmt.Errorf("action[SetOSSAccelEvictionRule],clusterID[%v] vol:%v err:%v", c.Name, r.VolName, err)
	}
	c.ossAccelEvictionRuleCache.Put(r)
	return nil
}

// GetOSSAccelEvictionRule returns the volume's rule, or nil if unset.
func (c *Cluster) GetOSSAccelEvictionRule(volName string) *proto.OSSAccelEvictionRule {
	return c.ossAccelEvictionRuleCache.Get(volName)
}

// DeleteOSSAccelEvictionRule removes the volume's eviction schedule.
func (c *Cluster) DeleteOSSAccelEvictionRule(volName string) error {
	r := &proto.OSSAccelEvictionRule{VolName: volName}
	if err := c.syncDeleteOSSAccelEvictionRule(r); err != nil {
		return fmt.Errorf("action[DeleteOSSAccelEvictionRule],clusterID[%v] vol:%v err:%v", c.Name, volName, err)
	}
	c.ossAccelEvictionRuleCache.Delete(volName)
	return nil
}
