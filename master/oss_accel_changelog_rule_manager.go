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

// M2 production automation — master-side scheduler for oss-accel
// changelog rules. Deliberately a plain time.Ticker, not robfig/cron
// (SyncRuleManager's engine): changelog sync is a continuous catch-up
// poll ("every N seconds"), not a calendar-scheduled job, so cron's
// date/time semantics buy nothing here.
//
// Dispatch reuses the EXACT same AdminTask/TaskManager primitive as
// lifecycle scanning (Cluster.addLcNodeTasks, master/cluster_task.go:60)
// — not a new master→lcnode HTTP call path. Unlike lifecycle scanning,
// there's no idleLcNodeCh backpressure queue: a changelog sync task is a
// fast, idempotent, per-volume operation, so any currently active lcnode
// is an acceptable target (simple round-robin spread, not a scheduled
// hand-off).
//
// Lifecycle mirrors SyncRuleManager / lifecycleManager: Start on raft
// leader gain, Stop (+ rebuild) on leader loss (master_manager.go).

// OSSAccelChangelogRuleManager schedules and dispatches changelog sync
// tasks across the lcnode fleet. One instance per Cluster.
type OSSAccelChangelogRuleManager struct {
	cluster *Cluster

	mu      sync.Mutex
	stopCh  chan struct{}
	started bool

	nextNodeIdx uint64 // atomic round-robin index for pickActiveLcNode
}

// ossAccelChangelogRuleTickInterval is how often the manager re-checks
// every rule's due time. Independent of any individual rule's
// IntervalSeconds — a rule due every 30s still only gets checked (and
// possibly fired) on this cadence, so IntervalSeconds smaller than this
// tick has no effect finer than the tick itself. 10s keeps the check
// cheap (an in-memory map scan) while staying well under any interval an
// operator would realistically configure.
const ossAccelChangelogRuleTickInterval = 10 * time.Second

func NewOSSAccelChangelogRuleManager(cluster *Cluster) *OSSAccelChangelogRuleManager {
	return &OSSAccelChangelogRuleManager{cluster: cluster}
}

// Start begins the ticker loop. Idempotent: re-calling without an
// intervening Stop is a no-op.
func (m *OSSAccelChangelogRuleManager) Start() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.started {
		return
	}
	m.stopCh = make(chan struct{})
	m.started = true
	go m.run(m.stopCh)
	log.LogInfo("OSSAccelChangelogRuleManager.Start")
}

// Stop halts the ticker loop. Idempotent.
func (m *OSSAccelChangelogRuleManager) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.started {
		return
	}
	close(m.stopCh)
	m.started = false
	log.LogInfo("OSSAccelChangelogRuleManager.Stop")
}

func (m *OSSAccelChangelogRuleManager) run(stopCh chan struct{}) {
	ticker := time.NewTicker(ossAccelChangelogRuleTickInterval)
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
func (m *OSSAccelChangelogRuleManager) tick() {
	now := time.Now()
	for _, r := range m.cluster.ossAccelChangelogRuleCache.List() {
		if !r.Enabled || r.IntervalSeconds == 0 {
			continue
		}
		if !r.LastRunAt.IsZero() && now.Sub(r.LastRunAt) < time.Duration(r.IntervalSeconds)*time.Second {
			continue
		}
		m.fireRule(r)
	}
}

// fireRule dispatches one changelog sync task for r's volume to a live
// lcnode. r is a cache-owned pointer (read-only per OSSAccelChangelogRuleCache's
// contract) — the LastRunAt stamp is applied to a copy before persisting.
func (m *OSSAccelChangelogRuleManager) fireRule(r *proto.OSSAccelChangelogRule) {
	nodeAddr := m.pickActiveLcNode()
	if nodeAddr == "" {
		log.LogWarnf("OSSAccelChangelogRuleManager.fireRule: vol(%v) no active lcnode available, skip this tick", r.VolName)
		return
	}
	lcNode, err := m.cluster.lcNode(nodeAddr)
	if err != nil {
		log.LogWarnf("OSSAccelChangelogRuleManager.fireRule: vol(%v) lcNode(%v) lookup err: %v", r.VolName, nodeAddr, err)
		return
	}
	req := &proto.OSSAccelChangelogSyncTaskRequest{
		MasterAddr:            m.cluster.masterAddr(),
		LcNodeAddr:            lcNode.Addr,
		VolName:               r.VolName,
		Prefix:                r.Prefix,
		ChangelogKey:          r.ChangelogKey,
		SkipAfterFailures:     r.SkipAfterFailures,
		ConsecutiveFailures:   r.ConsecutiveFailures,
		PlaceholderTTLSeconds: r.PlaceholderTTLSeconds,
	}
	task := proto.NewAdminTaskEx(proto.OpLcNodeOssAccelChangelogSync, lcNode.Addr, req, r.VolName)
	m.cluster.addLcNodeTasks([]*proto.AdminTask{task})

	// Optimistic stamp so a slow-to-respond task doesn't get re-fired
	// every tick while in flight — mirrors lifecycleManager.process()'s
	// own optimistic AddResult-at-dispatch-time bookkeeping
	// (master/lifecycle_manager.go). The real outcome overwrites this
	// once lcnode's response lands (handleLcNodeOssAccelChangelogSyncResp,
	// master/lifecycle_task.go). If the task is lost entirely (crashed
	// lcnode, dropped connection), the rule just stays quiet until an
	// operator notices LastRunResult isn't advancing — the same accepted
	// "no automatic retry queue" tradeoff already documented for a
	// persistently failing changelog line (design doc §5), not a new risk.
	updated := *r
	updated.LastRunAt = time.Now()
	if perr := m.cluster.syncUpdateOSSAccelChangelogRule(&updated); perr != nil {
		log.LogWarnf("OSSAccelChangelogRuleManager.fireRule: vol(%v) persist LastRunAt err: %v", r.VolName, perr)
	}
	m.cluster.ossAccelChangelogRuleCache.Put(&updated)
	log.LogInfof("OSSAccelChangelogRuleManager.fireRule: dispatched vol(%v) to lcnode(%v)", r.VolName, nodeAddr)
}

// pickActiveLcNode returns some active lcnode address via simple
// round-robin over cluster.lcNodes, or "" if none are active. Changelog
// sync doesn't need lifecycle scanning's idleLcNodeCh backpressure queue
// (see package doc comment) — just enough spread to avoid always hammering
// the same node.
func (m *OSSAccelChangelogRuleManager) pickActiveLcNode() string {
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
	sort.Strings(addrs) // stable order so round-robin is deterministic across ticks
	idx := atomic.AddUint64(&m.nextNodeIdx, 1)
	return addrs[idx%uint64(len(addrs))]
}
