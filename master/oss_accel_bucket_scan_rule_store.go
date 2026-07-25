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

// Master raft persistence for the oss-accel bucket-scan rule — one schedule per
// volume, mirrors oss_accel_changelog_rule_store.go exactly (see that
// file's doc comment for why this is a per-vol single-value model, not
// SyncRule's independent-ID model).

const (
	ossAccelBucketScanRuleAcronym = "oabs"
	ossAccelBucketScanRulePrefix  = keySeparator + ossAccelBucketScanRuleAcronym + keySeparator
)

// OSSAccelBucketScanRuleCache holds the master's in-memory view of every
// persisted per-volume rule, keyed by volume name.
type OSSAccelBucketScanRuleCache struct {
	mu    sync.RWMutex
	rules map[string]*proto.OSSAccelBucketScanRule // volName -> rule
}

func NewOSSAccelBucketScanRuleCache() *OSSAccelBucketScanRuleCache {
	return &OSSAccelBucketScanRuleCache{rules: make(map[string]*proto.OSSAccelBucketScanRule)}
}

func (c *OSSAccelBucketScanRuleCache) Get(volName string) *proto.OSSAccelBucketScanRule {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.rules[volName]
}

// List returns a snapshot slice of every rule. The rules are returned by
// reference and MUST be treated as read-only by callers.
func (c *OSSAccelBucketScanRuleCache) List() []*proto.OSSAccelBucketScanRule {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]*proto.OSSAccelBucketScanRule, 0, len(c.rules))
	for _, r := range c.rules {
		out = append(out, r)
	}
	return out
}

func (c *OSSAccelBucketScanRuleCache) Put(r *proto.OSSAccelBucketScanRule) {
	if c == nil || r == nil {
		return
	}
	c.mu.Lock()
	c.rules[r.VolName] = r
	c.mu.Unlock()
}

func (c *OSSAccelBucketScanRuleCache) Delete(volName string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	delete(c.rules, volName)
	c.mu.Unlock()
}

func (c *Cluster) syncAddOSSAccelBucketScanRule(r *proto.OSSAccelBucketScanRule) error {
	return c.syncPutOSSAccelBucketScanRuleInfo(opSyncAddOSSAccelBucketScanRule, r)
}

func (c *Cluster) syncUpdateOSSAccelBucketScanRule(r *proto.OSSAccelBucketScanRule) error {
	return c.syncPutOSSAccelBucketScanRuleInfo(opSyncUpdateOSSAccelBucketScanRule, r)
}

func (c *Cluster) syncDeleteOSSAccelBucketScanRule(r *proto.OSSAccelBucketScanRule) error {
	return c.syncPutOSSAccelBucketScanRuleInfo(opSyncDeleteOSSAccelBucketScanRule, r)
}

func (c *Cluster) syncPutOSSAccelBucketScanRuleInfo(opType uint32, r *proto.OSSAccelBucketScanRule) error {
	if r == nil || r.VolName == "" {
		return fmt.Errorf("syncPutOSSAccelBucketScanRuleInfo: nil rule or empty volName")
	}
	metadata := new(RaftCmd)
	metadata.Op = opType
	metadata.K = ossAccelBucketScanRulePrefix + r.VolName
	var err error
	metadata.V, err = json.Marshal(r)
	if err != nil {
		return fmt.Errorf("syncPutOSSAccelBucketScanRuleInfo marshal vol=%s: %w", r.VolName, err)
	}
	return c.submit(metadata)
}

// loadOSSAccelBucketScanRules reconstructs the in-memory cache from the raft
// store on master start (cold load) and on leader switch. Must be called
// AFTER c.ossAccelBucketScanRuleCache has been assigned.
func (c *Cluster) loadOSSAccelBucketScanRules() (err error) {
	if c.ossAccelBucketScanRuleCache == nil {
		c.ossAccelBucketScanRuleCache = NewOSSAccelBucketScanRuleCache()
	}
	result, err := c.fsm.store.SeekForPrefix([]byte(ossAccelBucketScanRulePrefix))
	if err != nil {
		return fmt.Errorf("action[loadOSSAccelBucketScanRules],err:%v", err.Error())
	}
	loaded := 0
	for k, value := range result {
		rule := &proto.OSSAccelBucketScanRule{}
		if err = json.Unmarshal(value, rule); err != nil {
			log.LogErrorf("action[loadOSSAccelBucketScanRules],key:%s unmarshal err:%v", k, err)
			err = nil
			continue
		}
		if rule.VolName == "" {
			log.LogWarnf("action[loadOSSAccelBucketScanRules],key:%s rule has empty volName, skipping", k)
			continue
		}
		c.ossAccelBucketScanRuleCache.Put(rule)
		loaded++
	}
	log.LogInfof("action[loadOSSAccelBucketScanRules], loaded %d of %d records", loaded, len(result))
	return
}

// SetOSSAccelBucketScanRule creates or replaces the volume's bucket-scan schedule.
func (c *Cluster) SetOSSAccelBucketScanRule(r *proto.OSSAccelBucketScanRule) error {
	var err error
	if c.ossAccelBucketScanRuleCache.Get(r.VolName) != nil {
		err = c.syncUpdateOSSAccelBucketScanRule(r)
	} else {
		err = c.syncAddOSSAccelBucketScanRule(r)
	}
	if err != nil {
		return fmt.Errorf("action[SetOSSAccelBucketScanRule],clusterID[%v] vol:%v err:%v", c.Name, r.VolName, err)
	}
	c.ossAccelBucketScanRuleCache.Put(r)
	return nil
}

// GetOSSAccelBucketScanRule returns the volume's rule, or nil if unset.
func (c *Cluster) GetOSSAccelBucketScanRule(volName string) *proto.OSSAccelBucketScanRule {
	return c.ossAccelBucketScanRuleCache.Get(volName)
}

// DeleteOSSAccelBucketScanRule removes the volume's bucket-scan schedule.
func (c *Cluster) DeleteOSSAccelBucketScanRule(volName string) error {
	r := &proto.OSSAccelBucketScanRule{VolName: volName}
	if err := c.syncDeleteOSSAccelBucketScanRule(r); err != nil {
		return fmt.Errorf("action[DeleteOSSAccelBucketScanRule],clusterID[%v] vol:%v err:%v", c.Name, volName, err)
	}
	c.ossAccelBucketScanRuleCache.Delete(volName)
	return nil
}
