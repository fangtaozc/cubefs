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
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

// 系统层面收尾续(补1+3) — master-side scheduler for oss-accel flush-policy
// rules (age-triggered auto flush+commit-cold of files that have NEVER been
// tiered out before — orthogonal to OSSAccelEvictionRuleManager, which only
// re-cools files that were already flushed at some point). The tick's FIRE
// CONDITION is a plain elapsed-time check (like OSSAccelAuditRuleManager),
// not a watermark — but unlike audit, this rule DOES need an in-flight flag
// (see proto.OSSAccelFlushPolicyRule.FlushPolicyInFlight's doc comment for
// the real-machine incident that made this necessary: commit-cold's
// migration-slot handling is not safe under two concurrent calls on the same
// inode, which a too-short IntervalSeconds can trigger). Structurally a
// hybrid: OSSAccelAuditRuleManager's elapsed-time tick +
// OSSAccelEvictionRuleManager's in-flight-flag/watchdog dispatch guard.
//
// Dispatch reuses the EXACT same AdminTask/TaskManager primitive as
// changelog sync/eviction/audit — not a new master→lcnode HTTP call path.

// OSSAccelFlushPolicyRuleManager schedules and dispatches flush-policy tasks across the
// lcnode fleet. One instance per Cluster.
type OSSAccelFlushPolicyRuleManager struct {
	cluster *Cluster

	mu      sync.Mutex
	stopCh  chan struct{}
	started bool

	nextNodeIdx uint64 // atomic round-robin index for pickActiveLcNode
}

// ossAccelFlushPolicyRuleTickInterval mirrors ossAccelChangelogRuleTickInterval's
// rationale exactly (see that constant's doc comment).
const ossAccelFlushPolicyRuleTickInterval = 10 * time.Second

// ossAccelFlushPolicyDispatchStaleTimeout mirrors
// ossAccelEvictionDispatchStaleTimeout's rationale and value exactly (see
// that constant's doc comment) — same watchdog escape hatch for a
// lost/crashed lcnode leaving FlushPolicyInFlight stuck true forever.
const ossAccelFlushPolicyDispatchStaleTimeout = 10 * time.Minute

func NewOSSAccelFlushPolicyRuleManager(cluster *Cluster) *OSSAccelFlushPolicyRuleManager {
	return &OSSAccelFlushPolicyRuleManager{cluster: cluster}
}

// Start begins the ticker loop. Idempotent.
func (m *OSSAccelFlushPolicyRuleManager) Start() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.started {
		return
	}
	m.stopCh = make(chan struct{})
	m.started = true
	go m.run(m.stopCh)
	log.LogInfo("OSSAccelFlushPolicyRuleManager.Start")
}

// Stop halts the ticker loop. Idempotent.
func (m *OSSAccelFlushPolicyRuleManager) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.started {
		return
	}
	close(m.stopCh)
	m.started = false
	log.LogInfo("OSSAccelFlushPolicyRuleManager.Stop")
}

func (m *OSSAccelFlushPolicyRuleManager) run(stopCh chan struct{}) {
	ticker := time.NewTicker(ossAccelFlushPolicyRuleTickInterval)
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
func (m *OSSAccelFlushPolicyRuleManager) tick() {
	now := time.Now()
	for _, r := range m.cluster.ossAccelFlushPolicyRuleCache.List() {
		if !r.Enabled || r.IntervalSeconds == 0 {
			continue
		}
		if r.FlushPolicyInFlight {
			if time.Since(r.LastRunAt) < ossAccelFlushPolicyDispatchStaleTimeout {
				continue // a sweep is already running for this vol — don't pile up a second dispatch (the real-machine incident this guards against)
			}
			// Dispatched longer ago than any plausible legitimate sweep — the
			// lcnode it went to crashed or the task was lost in transit.
			// Mirrors OSSAccelEvictionRuleManager.tick's watchdog exactly.
			log.LogWarnf("OSSAccelFlushPolicyRuleManager.tick: vol(%v) FlushPolicyInFlight stuck true for over %v (dispatched at %v) with no lcnode response — treating as lost/crashed, auto-clearing",
				r.VolName, ossAccelFlushPolicyDispatchStaleTimeout, r.LastRunAt)
			// 跟HTTP setOSSAccelFlushPolicyRule handler共用同一把updateMu。
			m.cluster.ossAccelFlushPolicyRuleCache.LockUpdate()
			stale := *r
			stale.FlushPolicyInFlight = false
			stale.LastRunResult = fmt.Sprintf("stale: no lcnode response within %v of dispatch — auto-cleared by watchdog", ossAccelFlushPolicyDispatchStaleTimeout)
			m.cluster.ossAccelFlushPolicyRuleCache.Put(&stale)
			if perr := m.cluster.syncUpdateOSSAccelFlushPolicyRule(&stale); perr != nil {
				log.LogWarnf("OSSAccelFlushPolicyRuleManager.tick: vol(%v) persist stale-clear err: %v", r.VolName, perr)
			}
			m.cluster.ossAccelFlushPolicyRuleCache.UnlockUpdate()
			r = &stale
		}
		if !r.LastRunAt.IsZero() && now.Sub(r.LastRunAt) < time.Duration(r.IntervalSeconds)*time.Second {
			continue
		}
		m.fireRule(r)
	}
}

// fireRule dispatches one flush-policy task for r's volume to a live lcnode.
//
// FlushPolicyInFlight is flipped in the LOCAL CACHE first, before any
// dispatch I/O — mirrors OSSAccelEvictionRuleManager.fireRule's exact
// ordering rationale (see that method's doc comment): the cache write is
// synchronous and in-memory, so it's visible to the very next tick()
// regardless of how long the raft persist or the dispatch call take.
func (m *OSSAccelFlushPolicyRuleManager) fireRule(r *proto.OSSAccelFlushPolicyRule) {
	nodeAddr := m.pickActiveLcNode()
	if nodeAddr == "" {
		log.LogWarnf("OSSAccelFlushPolicyRuleManager.fireRule: vol(%v) no active lcnode available, skip this tick", r.VolName)
		return
	}
	lcNode, err := m.cluster.lcNode(nodeAddr)
	if err != nil {
		log.LogWarnf("OSSAccelFlushPolicyRuleManager.fireRule: vol(%v) lcNode(%v) lookup err: %v", r.VolName, nodeAddr, err)
		return
	}

	// 跟HTTP setOSSAccelFlushPolicyRule handler共用同一把updateMu。
	m.cluster.ossAccelFlushPolicyRuleCache.LockUpdate()
	updated := *r
	updated.FlushPolicyInFlight = true
	updated.LastRunAt = time.Now()
	m.cluster.ossAccelFlushPolicyRuleCache.Put(&updated)
	if perr := m.cluster.syncUpdateOSSAccelFlushPolicyRule(&updated); perr != nil {
		log.LogWarnf("OSSAccelFlushPolicyRuleManager.fireRule: vol(%v) persist FlushPolicyInFlight err: %v", r.VolName, perr)
	}
	m.cluster.ossAccelFlushPolicyRuleCache.UnlockUpdate()

	req := &proto.OSSAccelFlushPolicyTaskRequest{
		MasterAddr:   m.cluster.masterAddr(),
		LcNodeAddr:   lcNode.Addr,
		VolName:      r.VolName,
		Prefix:       r.Prefix,
		MinIdleHours: r.MinIdleHours,
		MinSizeBytes: r.MinSizeBytes,
	}
	task := proto.NewAdminTaskEx(proto.OpLcNodeOssAccelFlushPolicy, lcNode.Addr, req, r.VolName)
	m.cluster.addLcNodeTasks([]*proto.AdminTask{task})
	log.LogInfof("OSSAccelFlushPolicyRuleManager.fireRule: dispatched vol(%v) to lcnode(%v)", r.VolName, nodeAddr)
}

// pickActiveLcNode mirrors OSSAccelChangelogRuleManager's implementation
// exactly (see that method's doc comment).
func (m *OSSAccelFlushPolicyRuleManager) pickActiveLcNode() string {
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
