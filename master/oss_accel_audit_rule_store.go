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

// Master raft persistence for the oss-accel audit rule — one schedule per
// volume, mirrors oss_accel_changelog_rule_store.go exactly (see that
// file's doc comment for why this is a per-vol single-value model, not
// SyncRule's independent-ID model).

const (
	ossAccelAuditRuleAcronym = "oaar"
	ossAccelAuditRulePrefix  = keySeparator + ossAccelAuditRuleAcronym + keySeparator
)

// OSSAccelAuditRuleCache holds the master's in-memory view of every
// persisted per-volume rule, keyed by volume name.
type OSSAccelAuditRuleCache struct {
	mu    sync.RWMutex
	rules map[string]*proto.OSSAccelAuditRule // volName -> rule
}

func NewOSSAccelAuditRuleCache() *OSSAccelAuditRuleCache {
	return &OSSAccelAuditRuleCache{rules: make(map[string]*proto.OSSAccelAuditRule)}
}

func (c *OSSAccelAuditRuleCache) Get(volName string) *proto.OSSAccelAuditRule {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.rules[volName]
}

// List returns a snapshot slice of every rule. The rules are returned by
// reference and MUST be treated as read-only by callers.
func (c *OSSAccelAuditRuleCache) List() []*proto.OSSAccelAuditRule {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]*proto.OSSAccelAuditRule, 0, len(c.rules))
	for _, r := range c.rules {
		out = append(out, r)
	}
	return out
}

func (c *OSSAccelAuditRuleCache) Put(r *proto.OSSAccelAuditRule) {
	if c == nil || r == nil {
		return
	}
	c.mu.Lock()
	c.rules[r.VolName] = r
	c.mu.Unlock()
}

func (c *OSSAccelAuditRuleCache) Delete(volName string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	delete(c.rules, volName)
	c.mu.Unlock()
}

func (c *Cluster) syncAddOSSAccelAuditRule(r *proto.OSSAccelAuditRule) error {
	return c.syncPutOSSAccelAuditRuleInfo(opSyncAddOSSAccelAuditRule, r)
}

func (c *Cluster) syncUpdateOSSAccelAuditRule(r *proto.OSSAccelAuditRule) error {
	return c.syncPutOSSAccelAuditRuleInfo(opSyncUpdateOSSAccelAuditRule, r)
}

func (c *Cluster) syncDeleteOSSAccelAuditRule(r *proto.OSSAccelAuditRule) error {
	return c.syncPutOSSAccelAuditRuleInfo(opSyncDeleteOSSAccelAuditRule, r)
}

func (c *Cluster) syncPutOSSAccelAuditRuleInfo(opType uint32, r *proto.OSSAccelAuditRule) error {
	if r == nil || r.VolName == "" {
		return fmt.Errorf("syncPutOSSAccelAuditRuleInfo: nil rule or empty volName")
	}
	metadata := new(RaftCmd)
	metadata.Op = opType
	metadata.K = ossAccelAuditRulePrefix + r.VolName
	var err error
	metadata.V, err = json.Marshal(r)
	if err != nil {
		return fmt.Errorf("syncPutOSSAccelAuditRuleInfo marshal vol=%s: %w", r.VolName, err)
	}
	return c.submit(metadata)
}

// loadOSSAccelAuditRules reconstructs the in-memory cache from the raft
// store on master start (cold load) and on leader switch. Must be called
// AFTER c.ossAccelAuditRuleCache has been assigned.
func (c *Cluster) loadOSSAccelAuditRules() (err error) {
	if c.ossAccelAuditRuleCache == nil {
		c.ossAccelAuditRuleCache = NewOSSAccelAuditRuleCache()
	}
	result, err := c.fsm.store.SeekForPrefix([]byte(ossAccelAuditRulePrefix))
	if err != nil {
		return fmt.Errorf("action[loadOSSAccelAuditRules],err:%v", err.Error())
	}
	loaded := 0
	for k, value := range result {
		rule := &proto.OSSAccelAuditRule{}
		if err = json.Unmarshal(value, rule); err != nil {
			log.LogErrorf("action[loadOSSAccelAuditRules],key:%s unmarshal err:%v", k, err)
			err = nil
			continue
		}
		if rule.VolName == "" {
			log.LogWarnf("action[loadOSSAccelAuditRules],key:%s rule has empty volName, skipping", k)
			continue
		}
		c.ossAccelAuditRuleCache.Put(rule)
		loaded++
	}
	log.LogInfof("action[loadOSSAccelAuditRules], loaded %d of %d records", loaded, len(result))
	return
}

// SetOSSAccelAuditRule creates or replaces the volume's audit schedule.
func (c *Cluster) SetOSSAccelAuditRule(r *proto.OSSAccelAuditRule) error {
	var err error
	if c.ossAccelAuditRuleCache.Get(r.VolName) != nil {
		err = c.syncUpdateOSSAccelAuditRule(r)
	} else {
		err = c.syncAddOSSAccelAuditRule(r)
	}
	if err != nil {
		return fmt.Errorf("action[SetOSSAccelAuditRule],clusterID[%v] vol:%v err:%v", c.Name, r.VolName, err)
	}
	c.ossAccelAuditRuleCache.Put(r)
	return nil
}

// GetOSSAccelAuditRule returns the volume's rule, or nil if unset.
func (c *Cluster) GetOSSAccelAuditRule(volName string) *proto.OSSAccelAuditRule {
	return c.ossAccelAuditRuleCache.Get(volName)
}

// DeleteOSSAccelAuditRule removes the volume's audit schedule.
func (c *Cluster) DeleteOSSAccelAuditRule(volName string) error {
	r := &proto.OSSAccelAuditRule{VolName: volName}
	if err := c.syncDeleteOSSAccelAuditRule(r); err != nil {
		return fmt.Errorf("action[DeleteOSSAccelAuditRule],clusterID[%v] vol:%v err:%v", c.Name, volName, err)
	}
	c.ossAccelAuditRuleCache.Delete(volName)
	return nil
}
