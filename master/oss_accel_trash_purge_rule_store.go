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

// Master raft persistence for the oss-accel trash purge rule — one
// schedule per volume, mirrors oss_accel_changelog_rule_store.go exactly.

const (
	ossAccelTrashPurgeRuleAcronym = "oatp"
	ossAccelTrashPurgeRulePrefix  = keySeparator + ossAccelTrashPurgeRuleAcronym + keySeparator
)

// OSSAccelTrashPurgeRuleCache holds the master's in-memory view of every
// persisted per-volume rule, keyed by volume name.
type OSSAccelTrashPurgeRuleCache struct {
	mu    sync.RWMutex
	rules map[string]*proto.OSSAccelTrashPurgeRule // volName -> rule
}

func NewOSSAccelTrashPurgeRuleCache() *OSSAccelTrashPurgeRuleCache {
	return &OSSAccelTrashPurgeRuleCache{rules: make(map[string]*proto.OSSAccelTrashPurgeRule)}
}

func (c *OSSAccelTrashPurgeRuleCache) Get(volName string) *proto.OSSAccelTrashPurgeRule {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.rules[volName]
}

// List returns a snapshot slice of every rule. The rules are returned by
// reference and MUST be treated as read-only by callers.
func (c *OSSAccelTrashPurgeRuleCache) List() []*proto.OSSAccelTrashPurgeRule {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]*proto.OSSAccelTrashPurgeRule, 0, len(c.rules))
	for _, r := range c.rules {
		out = append(out, r)
	}
	return out
}

func (c *OSSAccelTrashPurgeRuleCache) Put(r *proto.OSSAccelTrashPurgeRule) {
	if c == nil || r == nil {
		return
	}
	c.mu.Lock()
	c.rules[r.VolName] = r
	c.mu.Unlock()
}

func (c *OSSAccelTrashPurgeRuleCache) Delete(volName string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	delete(c.rules, volName)
	c.mu.Unlock()
}

func (c *Cluster) syncAddOSSAccelTrashPurgeRule(r *proto.OSSAccelTrashPurgeRule) error {
	return c.syncPutOSSAccelTrashPurgeRuleInfo(opSyncAddOSSAccelTrashPurgeRule, r)
}

func (c *Cluster) syncUpdateOSSAccelTrashPurgeRule(r *proto.OSSAccelTrashPurgeRule) error {
	return c.syncPutOSSAccelTrashPurgeRuleInfo(opSyncUpdateOSSAccelTrashPurgeRule, r)
}

func (c *Cluster) syncDeleteOSSAccelTrashPurgeRule(r *proto.OSSAccelTrashPurgeRule) error {
	return c.syncPutOSSAccelTrashPurgeRuleInfo(opSyncDeleteOSSAccelTrashPurgeRule, r)
}

func (c *Cluster) syncPutOSSAccelTrashPurgeRuleInfo(opType uint32, r *proto.OSSAccelTrashPurgeRule) error {
	if r == nil || r.VolName == "" {
		return fmt.Errorf("syncPutOSSAccelTrashPurgeRuleInfo: nil rule or empty volName")
	}
	metadata := new(RaftCmd)
	metadata.Op = opType
	metadata.K = ossAccelTrashPurgeRulePrefix + r.VolName
	var err error
	metadata.V, err = json.Marshal(r)
	if err != nil {
		return fmt.Errorf("syncPutOSSAccelTrashPurgeRuleInfo marshal vol=%s: %w", r.VolName, err)
	}
	return c.submit(metadata)
}

// loadOSSAccelTrashPurgeRules reconstructs the in-memory cache from the
// raft store on master start (cold load) and on leader switch. Must be
// called AFTER c.ossAccelTrashPurgeRuleCache has been assigned.
func (c *Cluster) loadOSSAccelTrashPurgeRules() (err error) {
	if c.ossAccelTrashPurgeRuleCache == nil {
		c.ossAccelTrashPurgeRuleCache = NewOSSAccelTrashPurgeRuleCache()
	}
	result, err := c.fsm.store.SeekForPrefix([]byte(ossAccelTrashPurgeRulePrefix))
	if err != nil {
		return fmt.Errorf("action[loadOSSAccelTrashPurgeRules],err:%v", err.Error())
	}
	loaded := 0
	for k, value := range result {
		rule := &proto.OSSAccelTrashPurgeRule{}
		if err = json.Unmarshal(value, rule); err != nil {
			log.LogErrorf("action[loadOSSAccelTrashPurgeRules],key:%s unmarshal err:%v", k, err)
			err = nil
			continue
		}
		if rule.VolName == "" {
			log.LogWarnf("action[loadOSSAccelTrashPurgeRules],key:%s rule has empty volName, skipping", k)
			continue
		}
		c.ossAccelTrashPurgeRuleCache.Put(rule)
		loaded++
	}
	log.LogInfof("action[loadOSSAccelTrashPurgeRules], loaded %d of %d records", loaded, len(result))
	return
}

// SetOSSAccelTrashPurgeRule creates or replaces the volume's trash purge
// schedule.
func (c *Cluster) SetOSSAccelTrashPurgeRule(r *proto.OSSAccelTrashPurgeRule) error {
	var err error
	if c.ossAccelTrashPurgeRuleCache.Get(r.VolName) != nil {
		err = c.syncUpdateOSSAccelTrashPurgeRule(r)
	} else {
		err = c.syncAddOSSAccelTrashPurgeRule(r)
	}
	if err != nil {
		return fmt.Errorf("action[SetOSSAccelTrashPurgeRule],clusterID[%v] vol:%v err:%v", c.Name, r.VolName, err)
	}
	c.ossAccelTrashPurgeRuleCache.Put(r)
	return nil
}

// GetOSSAccelTrashPurgeRule returns the volume's rule, or nil if unset.
func (c *Cluster) GetOSSAccelTrashPurgeRule(volName string) *proto.OSSAccelTrashPurgeRule {
	return c.ossAccelTrashPurgeRuleCache.Get(volName)
}

// DeleteOSSAccelTrashPurgeRule removes the volume's trash purge schedule.
func (c *Cluster) DeleteOSSAccelTrashPurgeRule(volName string) error {
	r := &proto.OSSAccelTrashPurgeRule{VolName: volName}
	if err := c.syncDeleteOSSAccelTrashPurgeRule(r); err != nil {
		return fmt.Errorf("action[DeleteOSSAccelTrashPurgeRule],clusterID[%v] vol:%v err:%v", c.Name, volName, err)
	}
	c.ossAccelTrashPurgeRuleCache.Delete(volName)
	return nil
}
