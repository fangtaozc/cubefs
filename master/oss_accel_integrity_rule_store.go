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

// Master raft persistence for the oss-accel integrity rule — one schedule per
// volume, mirrors oss_accel_changelog_rule_store.go exactly (see that
// file's doc comment for why this is a per-vol single-value model, not
// SyncRule's independent-ID model).

const (
	ossAccelIntegrityRuleAcronym = "oair"
	ossAccelIntegrityRulePrefix  = keySeparator + ossAccelIntegrityRuleAcronym + keySeparator
)

// OSSAccelIntegrityRuleCache holds the master's in-memory view of every
// persisted per-volume rule, keyed by volume name.
type OSSAccelIntegrityRuleCache struct {
	mu    sync.RWMutex
	rules map[string]*proto.OSSAccelIntegrityRule // volName -> rule
}

func NewOSSAccelIntegrityRuleCache() *OSSAccelIntegrityRuleCache {
	return &OSSAccelIntegrityRuleCache{rules: make(map[string]*proto.OSSAccelIntegrityRule)}
}

func (c *OSSAccelIntegrityRuleCache) Get(volName string) *proto.OSSAccelIntegrityRule {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.rules[volName]
}

// List returns a snapshot slice of every rule. The rules are returned by
// reference and MUST be treated as read-only by callers.
func (c *OSSAccelIntegrityRuleCache) List() []*proto.OSSAccelIntegrityRule {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]*proto.OSSAccelIntegrityRule, 0, len(c.rules))
	for _, r := range c.rules {
		out = append(out, r)
	}
	return out
}

func (c *OSSAccelIntegrityRuleCache) Put(r *proto.OSSAccelIntegrityRule) {
	if c == nil || r == nil {
		return
	}
	c.mu.Lock()
	c.rules[r.VolName] = r
	c.mu.Unlock()
}

func (c *OSSAccelIntegrityRuleCache) Delete(volName string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	delete(c.rules, volName)
	c.mu.Unlock()
}

func (c *Cluster) syncAddOSSAccelIntegrityRule(r *proto.OSSAccelIntegrityRule) error {
	return c.syncPutOSSAccelIntegrityRuleInfo(opSyncAddOSSAccelIntegrityRule, r)
}

func (c *Cluster) syncUpdateOSSAccelIntegrityRule(r *proto.OSSAccelIntegrityRule) error {
	return c.syncPutOSSAccelIntegrityRuleInfo(opSyncUpdateOSSAccelIntegrityRule, r)
}

func (c *Cluster) syncDeleteOSSAccelIntegrityRule(r *proto.OSSAccelIntegrityRule) error {
	return c.syncPutOSSAccelIntegrityRuleInfo(opSyncDeleteOSSAccelIntegrityRule, r)
}

func (c *Cluster) syncPutOSSAccelIntegrityRuleInfo(opType uint32, r *proto.OSSAccelIntegrityRule) error {
	if r == nil || r.VolName == "" {
		return fmt.Errorf("syncPutOSSAccelIntegrityRuleInfo: nil rule or empty volName")
	}
	metadata := new(RaftCmd)
	metadata.Op = opType
	metadata.K = ossAccelIntegrityRulePrefix + r.VolName
	var err error
	metadata.V, err = json.Marshal(r)
	if err != nil {
		return fmt.Errorf("syncPutOSSAccelIntegrityRuleInfo marshal vol=%s: %w", r.VolName, err)
	}
	return c.submit(metadata)
}

// loadOSSAccelIntegrityRules reconstructs the in-memory cache from the raft
// store on master start (cold load) and on leader switch. Must be called
// AFTER c.ossAccelIntegrityRuleCache has been assigned.
func (c *Cluster) loadOSSAccelIntegrityRules() (err error) {
	if c.ossAccelIntegrityRuleCache == nil {
		c.ossAccelIntegrityRuleCache = NewOSSAccelIntegrityRuleCache()
	}
	result, err := c.fsm.store.SeekForPrefix([]byte(ossAccelIntegrityRulePrefix))
	if err != nil {
		return fmt.Errorf("action[loadOSSAccelIntegrityRules],err:%v", err.Error())
	}
	loaded := 0
	for k, value := range result {
		rule := &proto.OSSAccelIntegrityRule{}
		if err = json.Unmarshal(value, rule); err != nil {
			log.LogErrorf("action[loadOSSAccelIntegrityRules],key:%s unmarshal err:%v", k, err)
			err = nil
			continue
		}
		if rule.VolName == "" {
			log.LogWarnf("action[loadOSSAccelIntegrityRules],key:%s rule has empty volName, skipping", k)
			continue
		}
		c.ossAccelIntegrityRuleCache.Put(rule)
		loaded++
	}
	log.LogInfof("action[loadOSSAccelIntegrityRules], loaded %d of %d records", loaded, len(result))
	return
}

// SetOSSAccelIntegrityRule creates or replaces the volume's integrity schedule.
func (c *Cluster) SetOSSAccelIntegrityRule(r *proto.OSSAccelIntegrityRule) error {
	var err error
	if c.ossAccelIntegrityRuleCache.Get(r.VolName) != nil {
		err = c.syncUpdateOSSAccelIntegrityRule(r)
	} else {
		err = c.syncAddOSSAccelIntegrityRule(r)
	}
	if err != nil {
		return fmt.Errorf("action[SetOSSAccelIntegrityRule],clusterID[%v] vol:%v err:%v", c.Name, r.VolName, err)
	}
	c.ossAccelIntegrityRuleCache.Put(r)
	return nil
}

// GetOSSAccelIntegrityRule returns the volume's rule, or nil if unset.
func (c *Cluster) GetOSSAccelIntegrityRule(volName string) *proto.OSSAccelIntegrityRule {
	return c.ossAccelIntegrityRuleCache.Get(volName)
}

// DeleteOSSAccelIntegrityRule removes the volume's integrity schedule.
func (c *Cluster) DeleteOSSAccelIntegrityRule(volName string) error {
	r := &proto.OSSAccelIntegrityRule{VolName: volName}
	if err := c.syncDeleteOSSAccelIntegrityRule(r); err != nil {
		return fmt.Errorf("action[DeleteOSSAccelIntegrityRule],clusterID[%v] vol:%v err:%v", c.Name, volName, err)
	}
	c.ossAccelIntegrityRuleCache.Delete(volName)
	return nil
}
