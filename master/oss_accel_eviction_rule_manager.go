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
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/log"
)

// M3 容量治理 — master-side water-level scheduler for oss-accel coldest-first
// eviction. Structurally identical to OSSAccelChangelogRuleManager (plain
// time.Ticker, dispatch via the same Cluster.addLcNodeTasks AdminTask
// primitive) — the only real difference is the FIRE CONDITION: a changelog
// rule fires on elapsed IntervalSeconds, this one fires when
// vol.totalUsedSpace()/(vol.capacity()*util.GB) crosses HighWatermarkRatio
// (master/vol.go:1108,1289 already expose both halves of that ratio).

// OSSAccelEvictionRuleManager schedules and dispatches eviction sweeps
// across the lcnode fleet. One instance per Cluster.
type OSSAccelEvictionRuleManager struct {
	cluster *Cluster

	mu      sync.Mutex
	stopCh  chan struct{}
	started bool

	nextNodeIdx uint64
}

// ossAccelEvictionRuleTickInterval: how often the manager re-checks every
// rule's volume usage ratio. A plain in-memory map scan + one Vol lookup
// per rule — cheap enough to check often; watermark crossings don't need
// sub-10s reaction time for this to be useful.
const ossAccelEvictionRuleTickInterval = 10 * time.Second

func NewOSSAccelEvictionRuleManager(cluster *Cluster) *OSSAccelEvictionRuleManager {
	return &OSSAccelEvictionRuleManager{cluster: cluster}
}

func (m *OSSAccelEvictionRuleManager) Start() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.started {
		return
	}
	m.stopCh = make(chan struct{})
	m.started = true
	go m.run(m.stopCh)
	log.LogInfo("OSSAccelEvictionRuleManager.Start")
}

func (m *OSSAccelEvictionRuleManager) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.started {
		return
	}
	close(m.stopCh)
	m.started = false
	log.LogInfo("OSSAccelEvictionRuleManager.Stop")
}

func (m *OSSAccelEvictionRuleManager) run(stopCh chan struct{}) {
	ticker := time.NewTicker(ossAccelEvictionRuleTickInterval)
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

func (m *OSSAccelEvictionRuleManager) tick() {
	for _, r := range m.cluster.ossAccelEvictionRuleCache.List() {
		if !r.Enabled || r.EvictionInFlight {
			continue // a sweep is already running for this vol — don't pile up a second dispatch
		}
		vol, err := m.cluster.getVol(r.VolName)
		if err != nil {
			log.LogWarnf("OSSAccelEvictionRuleManager.tick: vol(%v) getVol err: %v", r.VolName, err)
			continue
		}
		capacityBytes := vol.capacity() * util.GB
		if capacityBytes == 0 {
			continue
		}
		usageRatio := float64(vol.totalUsedSpace()) / float64(capacityBytes)
		if usageRatio < r.HighWatermarkRatio {
			continue
		}
		m.fireRule(r, usageRatio)
	}
}

// fireRule dispatches one eviction sweep for r's volume to a live lcnode.
func (m *OSSAccelEvictionRuleManager) fireRule(r *proto.OSSAccelEvictionRule, usageRatio float64) {
	nodeAddr := m.pickActiveLcNode()
	if nodeAddr == "" {
		log.LogWarnf("OSSAccelEvictionRuleManager.fireRule: vol(%v) no active lcnode available, skip this tick", r.VolName)
		return
	}
	lcNode, err := m.cluster.lcNode(nodeAddr)
	if err != nil {
		log.LogWarnf("OSSAccelEvictionRuleManager.fireRule: vol(%v) lcNode(%v) lookup err: %v", r.VolName, nodeAddr, err)
		return
	}
	req := &proto.OSSAccelEvictionTaskRequest{
		MasterAddr:        m.cluster.masterAddr(),
		LcNodeAddr:        lcNode.Addr,
		VolName:           r.VolName,
		LowWatermarkRatio: r.LowWatermarkRatio,
	}
	task := proto.NewAdminTaskEx(proto.OpLcNodeOssAccelEvict, lcNode.Addr, req, r.VolName)
	m.cluster.addLcNodeTasks([]*proto.AdminTask{task})

	updated := *r
	updated.EvictionInFlight = true
	updated.LastRunAt = time.Now()
	if perr := m.cluster.syncUpdateOSSAccelEvictionRule(&updated); perr != nil {
		log.LogWarnf("OSSAccelEvictionRuleManager.fireRule: vol(%v) persist EvictionInFlight err: %v", r.VolName, perr)
	}
	m.cluster.ossAccelEvictionRuleCache.Put(&updated)
	log.LogInfof("OSSAccelEvictionRuleManager.fireRule: vol(%v) usageRatio(%.4f) >= high(%.4f), dispatched to lcnode(%v)",
		r.VolName, usageRatio, r.HighWatermarkRatio, nodeAddr)
}

// pickActiveLcNode mirrors OSSAccelChangelogRuleManager's — see that file's
// doc comment for why a plain round-robin (no idleLcNodeCh backpressure
// queue) is the right choice here too.
func (m *OSSAccelEvictionRuleManager) pickActiveLcNode() string {
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
