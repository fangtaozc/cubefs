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
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

// 系统层面收尾(自动化程度不均) — master-side scheduler for oss-accel trash
// purge rules. Structurally identical to OSSAccelAuditRuleManager (plain
// elapsed-time ticker, no in-flight flag needed) — see that file's doc
// comment.

// OSSAccelTrashPurgeRuleManager schedules and dispatches trash purge tasks
// across the lcnode fleet. One instance per Cluster.
type OSSAccelTrashPurgeRuleManager struct {
	cluster *Cluster

	mu      sync.Mutex
	stopCh  chan struct{}
	started bool

	nextNodeIdx uint64 // atomic round-robin index for pickActiveLcNode
}

const ossAccelTrashPurgeRuleTickInterval = 10 * time.Second

func NewOSSAccelTrashPurgeRuleManager(cluster *Cluster) *OSSAccelTrashPurgeRuleManager {
	return &OSSAccelTrashPurgeRuleManager{cluster: cluster}
}

// Start begins the ticker loop. Idempotent.
func (m *OSSAccelTrashPurgeRuleManager) Start() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.started {
		return
	}
	m.stopCh = make(chan struct{})
	m.started = true
	go m.run(m.stopCh)
	log.LogInfo("OSSAccelTrashPurgeRuleManager.Start")
}

// Stop halts the ticker loop. Idempotent.
func (m *OSSAccelTrashPurgeRuleManager) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.started {
		return
	}
	close(m.stopCh)
	m.started = false
	log.LogInfo("OSSAccelTrashPurgeRuleManager.Stop")
}

func (m *OSSAccelTrashPurgeRuleManager) run(stopCh chan struct{}) {
	ticker := time.NewTicker(ossAccelTrashPurgeRuleTickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-stopCh:
			return
		case <-ticker.C:
			m.tick()
		}
	}
}

// tick scans every persisted rule and fires the ones that are due.
func (m *OSSAccelTrashPurgeRuleManager) tick() {
	now := time.Now()
	for _, r := range m.cluster.ossAccelTrashPurgeRuleCache.List() {
		if !r.Enabled || r.IntervalSeconds == 0 {
			continue
		}
		if !r.LastRunAt.IsZero() && now.Sub(r.LastRunAt) < time.Duration(r.IntervalSeconds)*time.Second {
			continue
		}
		m.fireRule(r)
	}
}

// fireRule dispatches one trash purge task for r's volume to a live lcnode.
func (m *OSSAccelTrashPurgeRuleManager) fireRule(r *proto.OSSAccelTrashPurgeRule) {
	nodeAddr := m.pickActiveLcNode()
	if nodeAddr == "" {
		log.LogWarnf("OSSAccelTrashPurgeRuleManager.fireRule: vol(%v) no active lcnode available, skip this tick", r.VolName)
		return
	}
	lcNode, err := m.cluster.lcNode(nodeAddr)
	if err != nil {
		log.LogWarnf("OSSAccelTrashPurgeRuleManager.fireRule: vol(%v) lcNode(%v) lookup err: %v", r.VolName, nodeAddr, err)
		return
	}
	req := &proto.OSSAccelTrashPurgeTaskRequest{
		MasterAddr:     m.cluster.masterAddr(),
		LcNodeAddr:     lcNode.Addr,
		VolName:        r.VolName,
		Prefix:         r.Prefix,
		RetentionHours: r.RetentionHours,
	}
	task := proto.NewAdminTaskEx(proto.OpLcNodeOssAccelTrashPurge, lcNode.Addr, req, r.VolName)
	m.cluster.addLcNodeTasks([]*proto.AdminTask{task})

	updated := *r
	updated.LastRunAt = time.Now()
	if perr := m.cluster.syncUpdateOSSAccelTrashPurgeRule(&updated); perr != nil {
		log.LogWarnf("OSSAccelTrashPurgeRuleManager.fireRule: vol(%v) persist LastRunAt err: %v", r.VolName, perr)
	}
	m.cluster.ossAccelTrashPurgeRuleCache.Put(&updated)
	log.LogInfof("OSSAccelTrashPurgeRuleManager.fireRule: dispatched vol(%v) to lcnode(%v)", r.VolName, nodeAddr)
}

// pickActiveLcNode mirrors OSSAccelChangelogRuleManager's implementation
// exactly (see that method's doc comment).
func (m *OSSAccelTrashPurgeRuleManager) pickActiveLcNode() string {
	var addrs []string
	m.cluster.lcNodes.Range(func(_, value interface{}) bool {
		if ln, ok := value.(*LcNode); ok && ln.IsActive {
			addrs = append(addrs, ln.Addr)
		}
		return true
	})
	if len(addrs) == 0 {
		return ""
	}
	sort.Strings(addrs)
	idx := atomic.AddUint64(&m.nextNodeIdx, 1)
	return addrs[idx%uint64(len(addrs))]
}
