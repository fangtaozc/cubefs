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
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/exporter"
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

// ossAccelEvictionDispatchStaleTimeout: EvictionInFlight 卡在 true 超过这个
// 时长仍未收到响应,视为 lcnode 崩溃/任务丢失(而不是"还在正常跑"),自动清
// 除标志位让水位判断继续生效——否则唯一能清除它的两条路径(收到真实响应/
// master 重启换主)都不会自然发生,规则会永久退出水位管理且不报错(真机验
// 证坐实过:网络分区掉线的 lcnode 收到派发后,EvictionInFlight 卡死数分钟
// 不会自愈)。10 分钟是保守估计,远大于 AdminTask 自身派发超时(100s)和节
// 点心跳超时(18s),不会跟"派发刚发出、响应还没回来"的正常窗口打架;没有
// 真实生产扫描耗时数据支撑这个数字(M3 设计文档已记录的 DEBT-3 缺口),如
// 果真出现扫描持续超过这个时长导致误判,需要现场调大,不是硬性约束。
const ossAccelEvictionDispatchStaleTimeout = 10 * time.Minute

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
		if !r.Enabled {
			continue
		}
		if r.EvictionInFlight {
			if time.Since(r.LastRunAt) < ossAccelEvictionDispatchStaleTimeout {
				continue // a sweep is already running for this vol — don't pile up a second dispatch
			}
			// Dispatched longer ago than any plausible legitimate sweep —
			// the lcnode it went to crashed or the task was lost in
			// transit (both leave EvictionInFlight stuck forever otherwise,
			// since a response is the only thing that clears it). Unstick
			// it and fall through to re-evaluate the watermark THIS tick,
			// not next — no need to wait another ossAccelEvictionRuleTickInterval.
			log.LogWarnf("OSSAccelEvictionRuleManager.tick: vol(%v) EvictionInFlight stuck true for over %v (dispatched at %v) with no lcnode response — treating as lost/crashed, auto-clearing",
				r.VolName, ossAccelEvictionDispatchStaleTimeout, r.LastRunAt)
			// 跟HTTP setOSSAccelEvictionRule handler共用同一把updateMu,避免
			// 这段Get(通过上面的List)+Put跟handler自己的Get+Put交错踩踏。
			m.cluster.ossAccelEvictionRuleCache.LockUpdate()
			stale := *r
			stale.EvictionInFlight = false
			stale.LastRunResult = fmt.Sprintf("stale: no lcnode response within %v of dispatch — auto-cleared by watchdog", ossAccelEvictionDispatchStaleTimeout)
			m.cluster.ossAccelEvictionRuleCache.Put(&stale)
			if perr := m.cluster.syncUpdateOSSAccelEvictionRule(&stale); perr != nil {
				log.LogWarnf("OSSAccelEvictionRuleManager.tick: vol(%v) persist stale-clear err: %v", r.VolName, perr)
			}
			m.cluster.ossAccelEvictionRuleCache.UnlockUpdate()
			r = &stale
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
		// 差距分析续(对照阿里云OSS加速器功能页): usageRatio 之前只在这里内部
		// 判断用,从没导出成外部可查的指标——运维没法从外部看"这个卷离驱逐线
		// 还有多远"。导出成本可忽略(每 tick 已经算出来的值,多发一次)。
		exporter.NewGauge("oss_accel_usage_ratio").SetWithLabels(usageRatio, map[string]string{exporter.Vol: r.VolName})
		if usageRatio < r.HighWatermarkRatio {
			continue
		}
		m.fireRule(r, usageRatio)
	}
}

// fireRule dispatches one eviction sweep for r's volume to a live lcnode.
//
// EvictionInFlight is flipped in the LOCAL CACHE first, before any dispatch
// I/O — real-machine testing found the original order (dispatch, then
// persist+cache) left a window where a sweep taking longer than one 10s
// tick let a LATER tick still read EvictionInFlight=false (raft persist
// hadn't landed / cache hadn't been updated yet) and fire a SECOND
// dispatch to a different round-robin lcnode for the SAME rule, both
// running the same sweep concurrently. The cache write is synchronous and
// in-memory, so it's visible to the very next tick() regardless of how
// long the raft persist or the dispatch call take.
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

	// 跟HTTP setOSSAccelEvictionRule handler共用同一把updateMu,理由见
	// OSSAccelEvictionRuleCache.updateMu的doc comment。
	m.cluster.ossAccelEvictionRuleCache.LockUpdate()
	updated := *r
	updated.EvictionInFlight = true
	updated.LastRunAt = time.Now()
	m.cluster.ossAccelEvictionRuleCache.Put(&updated)
	if perr := m.cluster.syncUpdateOSSAccelEvictionRule(&updated); perr != nil {
		log.LogWarnf("OSSAccelEvictionRuleManager.fireRule: vol(%v) persist EvictionInFlight err: %v", r.VolName, perr)
	}
	m.cluster.ossAccelEvictionRuleCache.UnlockUpdate()

	req := &proto.OSSAccelEvictionTaskRequest{
		MasterAddr:        m.cluster.masterAddr(),
		LcNodeAddr:        lcNode.Addr,
		VolName:           r.VolName,
		LowWatermarkRatio: r.LowWatermarkRatio,
		Order:             r.Order,
	}
	task := proto.NewAdminTaskEx(proto.OpLcNodeOssAccelEvict, lcNode.Addr, req, r.VolName)
	m.cluster.addLcNodeTasks([]*proto.AdminTask{task})
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
