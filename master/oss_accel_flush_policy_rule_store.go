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

// Master raft persistence for the oss-accel flush-policy rule — one schedule per
// volume, mirrors oss_accel_audit_rule_store.go exactly (see that
// file's doc comment for why this is a per-vol single-value model, not
// SyncRule's independent-ID model).

const (
	ossAccelFlushPolicyRuleAcronym = "oafp"
	ossAccelFlushPolicyRulePrefix  = keySeparator + ossAccelFlushPolicyRuleAcronym + keySeparator
)

// OSSAccelFlushPolicyRuleCache holds the master's in-memory view of every
// persisted per-volume rule, keyed by volume name.
type OSSAccelFlushPolicyRuleCache struct {
	mu    sync.RWMutex
	rules map[string]*proto.OSSAccelFlushPolicyRule // volName -> rule

	// updateMu 序列化"读现有规则→合并派发态字段→写回"这类复合操作,理由跟
	// OSSAccelEvictionRuleCache.updateMu完全一样(见那份doc comment)——HTTP
	// setOSSAccelFlushPolicyRule handler跟后台tick()/fireRule各自的
	// Get-then-Put序列需要共用这把锁才不会互相踩踏。
	updateMu sync.Mutex
}

func (c *OSSAccelFlushPolicyRuleCache) LockUpdate()   { c.updateMu.Lock() }
func (c *OSSAccelFlushPolicyRuleCache) UnlockUpdate() { c.updateMu.Unlock() }

func NewOSSAccelFlushPolicyRuleCache() *OSSAccelFlushPolicyRuleCache {
	return &OSSAccelFlushPolicyRuleCache{rules: make(map[string]*proto.OSSAccelFlushPolicyRule)}
}

func (c *OSSAccelFlushPolicyRuleCache) Get(volName string) *proto.OSSAccelFlushPolicyRule {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.rules[volName]
}

// List returns a snapshot slice of every rule. The rules are returned by
// reference and MUST be treated as read-only by callers.
func (c *OSSAccelFlushPolicyRuleCache) List() []*proto.OSSAccelFlushPolicyRule {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]*proto.OSSAccelFlushPolicyRule, 0, len(c.rules))
	for _, r := range c.rules {
		out = append(out, r)
	}
	return out
}

func (c *OSSAccelFlushPolicyRuleCache) Put(r *proto.OSSAccelFlushPolicyRule) {
	if c == nil || r == nil {
		return
	}
	c.mu.Lock()
	c.rules[r.VolName] = r
	c.mu.Unlock()
}

func (c *OSSAccelFlushPolicyRuleCache) Delete(volName string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	delete(c.rules, volName)
	c.mu.Unlock()
}

func (c *Cluster) syncAddOSSAccelFlushPolicyRule(r *proto.OSSAccelFlushPolicyRule) error {
	return c.syncPutOSSAccelFlushPolicyRuleInfo(opSyncAddOSSAccelFlushPolicyRule, r)
}

func (c *Cluster) syncUpdateOSSAccelFlushPolicyRule(r *proto.OSSAccelFlushPolicyRule) error {
	return c.syncPutOSSAccelFlushPolicyRuleInfo(opSyncUpdateOSSAccelFlushPolicyRule, r)
}

func (c *Cluster) syncDeleteOSSAccelFlushPolicyRule(r *proto.OSSAccelFlushPolicyRule) error {
	return c.syncPutOSSAccelFlushPolicyRuleInfo(opSyncDeleteOSSAccelFlushPolicyRule, r)
}

func (c *Cluster) syncPutOSSAccelFlushPolicyRuleInfo(opType uint32, r *proto.OSSAccelFlushPolicyRule) error {
	if r == nil || r.VolName == "" {
		return fmt.Errorf("syncPutOSSAccelFlushPolicyRuleInfo: nil rule or empty volName")
	}
	metadata := new(RaftCmd)
	metadata.Op = opType
	metadata.K = ossAccelFlushPolicyRulePrefix + r.VolName
	var err error
	metadata.V, err = json.Marshal(r)
	if err != nil {
		return fmt.Errorf("syncPutOSSAccelFlushPolicyRuleInfo marshal vol=%s: %w", r.VolName, err)
	}
	return c.submit(metadata)
}

// loadOSSAccelFlushPolicyRules reconstructs the in-memory cache from the raft
// store on master start (cold load) and on leader switch. Must be called
// AFTER c.ossAccelFlushPolicyRuleCache has been assigned.
func (c *Cluster) loadOSSAccelFlushPolicyRules() (err error) {
	if c.ossAccelFlushPolicyRuleCache == nil {
		c.ossAccelFlushPolicyRuleCache = NewOSSAccelFlushPolicyRuleCache()
	}
	result, err := c.fsm.store.SeekForPrefix([]byte(ossAccelFlushPolicyRulePrefix))
	if err != nil {
		return fmt.Errorf("action[loadOSSAccelFlushPolicyRules],err:%v", err.Error())
	}
	loaded := 0
	for k, value := range result {
		rule := &proto.OSSAccelFlushPolicyRule{}
		if err = json.Unmarshal(value, rule); err != nil {
			log.LogErrorf("action[loadOSSAccelFlushPolicyRules],key:%s unmarshal err:%v", k, err)
			err = nil
			continue
		}
		if rule.VolName == "" {
			log.LogWarnf("action[loadOSSAccelFlushPolicyRules],key:%s rule has empty volName, skipping", k)
			continue
		}
		c.ossAccelFlushPolicyRuleCache.Put(rule)
		loaded++
	}
	log.LogInfof("action[loadOSSAccelFlushPolicyRules], loaded %d of %d records", loaded, len(result))
	return
}

// SetOSSAccelFlushPolicyRule creates or replaces the volume's flush-policy schedule.
func (c *Cluster) SetOSSAccelFlushPolicyRule(r *proto.OSSAccelFlushPolicyRule) error {
	var err error
	if c.ossAccelFlushPolicyRuleCache.Get(r.VolName) != nil {
		err = c.syncUpdateOSSAccelFlushPolicyRule(r)
	} else {
		err = c.syncAddOSSAccelFlushPolicyRule(r)
	}
	if err != nil {
		return fmt.Errorf("action[SetOSSAccelFlushPolicyRule],clusterID[%v] vol:%v err:%v", c.Name, r.VolName, err)
	}
	c.ossAccelFlushPolicyRuleCache.Put(r)
	return nil
}

// GetOSSAccelFlushPolicyRule returns the volume's rule, or nil if unset.
func (c *Cluster) GetOSSAccelFlushPolicyRule(volName string) *proto.OSSAccelFlushPolicyRule {
	return c.ossAccelFlushPolicyRuleCache.Get(volName)
}

// DeleteOSSAccelFlushPolicyRule removes the volume's flush-policy schedule.
func (c *Cluster) DeleteOSSAccelFlushPolicyRule(volName string) error {
	r := &proto.OSSAccelFlushPolicyRule{VolName: volName}
	if err := c.syncDeleteOSSAccelFlushPolicyRule(r); err != nil {
		return fmt.Errorf("action[DeleteOSSAccelFlushPolicyRule],clusterID[%v] vol:%v err:%v", c.Name, volName, err)
	}
	c.ossAccelFlushPolicyRuleCache.Delete(volName)
	return nil
}
