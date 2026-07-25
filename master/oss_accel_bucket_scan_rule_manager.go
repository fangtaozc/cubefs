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

// 反向加速续(补两条发现路径) — master-side scheduler for periodic
// prefix-scoped S3 bucket-scan rules. Structurally identical to
// OSSAccelAuditRuleManager: a plain elapsed-time ticker, not a watermark
// trigger — a bucket-scan sweep is a periodic discovery pass ("every N
// seconds"), not something that fires off a resource crossing a threshold.
// No in-flight flag — see runOssAccelRegisterForVol's doc comment
// (lcnode/oss_accel_register.go) for why overlapping sweeps here self-heal
// (a clean Create_ll error on the losing side, never a hang), unlike
// flush-policy's commit-cold race that DID need one.
//
// Dispatch reuses the EXACT same AdminTask/TaskManager primitive as
// changelog sync/eviction/audit — not a new master→lcnode HTTP call path.

// OSSAccelBucketScanRuleManager schedules and dispatches bucket-scan tasks across the
// lcnode fleet. One instance per Cluster.
type OSSAccelBucketScanRuleManager struct {
	cluster *Cluster

	mu      sync.Mutex
	stopCh  chan struct{}
	started bool

	nextNodeIdx uint64 // atomic round-robin index for pickActiveLcNode
}

// ossAccelBucketScanRuleTickInterval mirrors ossAccelChangelogRuleTickInterval's
// rationale exactly (see that constant's doc comment).
const ossAccelBucketScanRuleTickInterval = 10 * time.Second

func NewOSSAccelBucketScanRuleManager(cluster *Cluster) *OSSAccelBucketScanRuleManager {
	return &OSSAccelBucketScanRuleManager{cluster: cluster}
}

// Start begins the ticker loop. Idempotent.
func (m *OSSAccelBucketScanRuleManager) Start() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.started {
		return
	}
	m.stopCh = make(chan struct{})
	m.started = true
	go m.run(m.stopCh)
	log.LogInfo("OSSAccelBucketScanRuleManager.Start")
}

// Stop halts the ticker loop. Idempotent.
func (m *OSSAccelBucketScanRuleManager) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.started {
		return
	}
	close(m.stopCh)
	m.started = false
	log.LogInfo("OSSAccelBucketScanRuleManager.Stop")
}

func (m *OSSAccelBucketScanRuleManager) run(stopCh chan struct{}) {
	ticker := time.NewTicker(ossAccelBucketScanRuleTickInterval)
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
func (m *OSSAccelBucketScanRuleManager) tick() {
	now := time.Now()
	for _, r := range m.cluster.ossAccelBucketScanRuleCache.List() {
		if !r.Enabled || r.IntervalSeconds == 0 {
			continue
		}
		if !r.LastRunAt.IsZero() && now.Sub(r.LastRunAt) < time.Duration(r.IntervalSeconds)*time.Second {
			continue
		}
		m.fireRule(r)
	}
}

// fireRule dispatches one bucket-scan task for r's volume to a live lcnode.
func (m *OSSAccelBucketScanRuleManager) fireRule(r *proto.OSSAccelBucketScanRule) {
	nodeAddr := m.pickActiveLcNode()
	if nodeAddr == "" {
		log.LogWarnf("OSSAccelBucketScanRuleManager.fireRule: vol(%v) no active lcnode available, skip this tick", r.VolName)
		return
	}
	lcNode, err := m.cluster.lcNode(nodeAddr)
	if err != nil {
		log.LogWarnf("OSSAccelBucketScanRuleManager.fireRule: vol(%v) lcNode(%v) lookup err: %v", r.VolName, nodeAddr, err)
		return
	}
	req := &proto.OSSAccelBucketScanTaskRequest{
		MasterAddr: m.cluster.masterAddr(),
		LcNodeAddr: lcNode.Addr,
		VolName:    r.VolName,
		Prefix:     r.Prefix,
	}
	task := proto.NewAdminTaskEx(proto.OpLcNodeOssAccelBucketScan, lcNode.Addr, req, r.VolName)
	m.cluster.addLcNodeTasks([]*proto.AdminTask{task})

	// Optimistic stamp so a slow-to-respond task doesn't get re-fired every
	// tick while in flight — same tradeoff OSSAccelChangelogRuleManager
	// documents (no automatic retry queue if the task is lost entirely).
	updated := *r
	updated.LastRunAt = time.Now()
	if perr := m.cluster.syncUpdateOSSAccelBucketScanRule(&updated); perr != nil {
		log.LogWarnf("OSSAccelBucketScanRuleManager.fireRule: vol(%v) persist LastRunAt err: %v", r.VolName, perr)
	}
	m.cluster.ossAccelBucketScanRuleCache.Put(&updated)
	log.LogInfof("OSSAccelBucketScanRuleManager.fireRule: dispatched vol(%v) to lcnode(%v)", r.VolName, nodeAddr)
}

// pickActiveLcNode mirrors OSSAccelChangelogRuleManager's implementation
// exactly (see that method's doc comment).
func (m *OSSAccelBucketScanRuleManager) pickActiveLcNode() string {
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
