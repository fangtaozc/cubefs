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

// Master raft persistence for the M2 oss-accel changelog rule — one
// schedule per volume. Mirrors the lifecycle config pattern
// (metadata_fsm_op.go:2180, lcConfPrefix+VolName) rather than SyncRule's
// independent-ID model (sync_rule_store.go): a volume either has a
// changelog sync schedule or it doesn't, so there's no separate
// list-of-IDs concept to manage.

const (
	ossAccelChangelogRuleAcronym = "oacr"
	ossAccelChangelogRulePrefix  = keySeparator + ossAccelChangelogRuleAcronym + keySeparator
)

// OSSAccelChangelogRuleCache holds the master's in-memory view of every
// persisted per-volume rule, keyed by volume name. Reads are lock-free
// relative to each other; writes go through the raft path first, then
// update this cache — mirrors SyncRuleCache (sync_rule_store.go).
type OSSAccelChangelogRuleCache struct {
	mu    sync.RWMutex
	rules map[string]*proto.OSSAccelChangelogRule // volName -> rule
}

func NewOSSAccelChangelogRuleCache() *OSSAccelChangelogRuleCache {
	return &OSSAccelChangelogRuleCache{rules: make(map[string]*proto.OSSAccelChangelogRule)}
}

func (c *OSSAccelChangelogRuleCache) Get(volName string) *proto.OSSAccelChangelogRule {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.rules[volName]
}

// List returns a snapshot slice of every rule. The rules are returned by
// reference and MUST be treated as read-only by callers.
func (c *OSSAccelChangelogRuleCache) List() []*proto.OSSAccelChangelogRule {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]*proto.OSSAccelChangelogRule, 0, len(c.rules))
	for _, r := range c.rules {
		out = append(out, r)
	}
	return out
}

func (c *OSSAccelChangelogRuleCache) Put(r *proto.OSSAccelChangelogRule) {
	if c == nil || r == nil {
		return
	}
	c.mu.Lock()
	c.rules[r.VolName] = r
	c.mu.Unlock()
}

func (c *OSSAccelChangelogRuleCache) Delete(volName string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	delete(c.rules, volName)
	c.mu.Unlock()
}

func (c *Cluster) syncAddOSSAccelChangelogRule(r *proto.OSSAccelChangelogRule) error {
	return c.syncPutOSSAccelChangelogRuleInfo(opSyncAddOSSAccelChangelogRule, r)
}

func (c *Cluster) syncUpdateOSSAccelChangelogRule(r *proto.OSSAccelChangelogRule) error {
	return c.syncPutOSSAccelChangelogRuleInfo(opSyncUpdateOSSAccelChangelogRule, r)
}

func (c *Cluster) syncDeleteOSSAccelChangelogRule(r *proto.OSSAccelChangelogRule) error {
	return c.syncPutOSSAccelChangelogRuleInfo(opSyncDeleteOSSAccelChangelogRule, r)
}

func (c *Cluster) syncPutOSSAccelChangelogRuleInfo(opType uint32, r *proto.OSSAccelChangelogRule) error {
	if r == nil || r.VolName == "" {
		return fmt.Errorf("syncPutOSSAccelChangelogRuleInfo: nil rule or empty volName")
	}
	metadata := new(RaftCmd)
	metadata.Op = opType
	metadata.K = ossAccelChangelogRulePrefix + r.VolName
	var err error
	metadata.V, err = json.Marshal(r)
	if err != nil {
		return fmt.Errorf("syncPutOSSAccelChangelogRuleInfo marshal vol=%s: %w", r.VolName, err)
	}
	return c.submit(metadata)
}

// loadOSSAccelChangelogRules reconstructs the in-memory cache from the raft
// store on master start (cold load) and on leader switch. Mirrors
// loadLcConfs (metadata_fsm_op.go:2203) / loadSyncRules (sync_rule_store.go).
// Must be called AFTER c.ossAccelChangelogRuleCache has been assigned.
func (c *Cluster) loadOSSAccelChangelogRules() (err error) {
	if c.ossAccelChangelogRuleCache == nil {
		c.ossAccelChangelogRuleCache = NewOSSAccelChangelogRuleCache()
	}
	result, err := c.fsm.store.SeekForPrefix([]byte(ossAccelChangelogRulePrefix))
	if err != nil {
		return fmt.Errorf("action[loadOSSAccelChangelogRules],err:%v", err.Error())
	}
	loaded := 0
	for k, value := range result {
		rule := &proto.OSSAccelChangelogRule{}
		if err = json.Unmarshal(value, rule); err != nil {
			log.LogErrorf("action[loadOSSAccelChangelogRules],key:%s unmarshal err:%v", k, err)
			err = nil
			continue
		}
		if rule.VolName == "" {
			log.LogWarnf("action[loadOSSAccelChangelogRules],key:%s rule has empty volName, skipping", k)
			continue
		}
		c.ossAccelChangelogRuleCache.Put(rule)
		loaded++
	}
	log.LogInfof("action[loadOSSAccelChangelogRules], loaded %d of %d records", loaded, len(result))
	return
}

// SetOSSAccelChangelogRule creates or replaces the volume's changelog sync
// schedule (mirrors Cluster.SetBucketLifecycle, master/cluster.go:6376).
func (c *Cluster) SetOSSAccelChangelogRule(r *proto.OSSAccelChangelogRule) error {
	var err error
	if c.ossAccelChangelogRuleCache.Get(r.VolName) != nil {
		err = c.syncUpdateOSSAccelChangelogRule(r)
	} else {
		err = c.syncAddOSSAccelChangelogRule(r)
	}
	if err != nil {
		return fmt.Errorf("action[SetOSSAccelChangelogRule],clusterID[%v] vol:%v err:%v", c.Name, r.VolName, err)
	}
	c.ossAccelChangelogRuleCache.Put(r)
	return nil
}

// GetOSSAccelChangelogRule returns the volume's rule, or nil if unset.
func (c *Cluster) GetOSSAccelChangelogRule(volName string) *proto.OSSAccelChangelogRule {
	return c.ossAccelChangelogRuleCache.Get(volName)
}

// DeleteOSSAccelChangelogRule removes the volume's changelog sync schedule.
func (c *Cluster) DeleteOSSAccelChangelogRule(volName string) error {
	r := &proto.OSSAccelChangelogRule{VolName: volName}
	if err := c.syncDeleteOSSAccelChangelogRule(r); err != nil {
		return fmt.Errorf("action[DeleteOSSAccelChangelogRule],clusterID[%v] vol:%v err:%v", c.Name, volName, err)
	}
	c.ossAccelChangelogRuleCache.Delete(volName)
	return nil
}
