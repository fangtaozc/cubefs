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

// 对齐AFM(队列观察第二轮): AFM's Queue Length is a single authoritative
// number because one fileset has exactly one active gateway and every
// home-communication operation passes through its one queue. oss-accel has
// no such mediator — lcnode is stateless anycast and flush/recall/
// commitCold/audit/integrity/eviction/trashPurge/bucketScan/changelogSync
// are independent mechanisms, each with its own state and cadence. A true
// single number covering all of them would require either cross-process RPC
// on every scrape (lcnode + objectnode are different binaries) or an
// unbounded-staleness poll loop, for no real benefit over what Prometheus
// already does at query time (`sum by (vol) (...)` across every existing
// gauge, refreshed at scrape time, correctly across processes).
//
// This file is the ONE piece of that aggregation that IS worth doing in Go:
// three sweeps that already live in this same lcnode process
// (flushPolicy/audit/trashPurge) can share a tiny in-memory cache and export
// a combined "how much lcnode-local backlog exists for this vol" number
// without any extra I/O. It deliberately excludes:
//   - recall in-flight: process-wide, not per-vol (see oss_accel_inflight_recalls'
//     own comment) — a different scope, mixing it into a per-vol sum would
//     misattribute cross-vol recall activity to whichever vol last updated.
//   - write-through-async in-flight: lives in the objectnode process, not
//     lcnode — combining it here would require an RPC this file exists
//     specifically to avoid.
//
// The `_lcnode` suffix on the exported gauge name is deliberate, not
// decorative: it is the flag that this is a PARTIAL, PROCESS-SCOPED
// estimate. Anyone wanting the full AFM-equivalent picture needs a
// Prometheus recording rule summing this gauge with oss_accel_inflight_recalls
// and objectnode's oss_accel_write_through_async_inflight — that composition
// belongs in the monitoring layer, not here.
package lcnode

import (
	"github.com/cubefs/cubefs/util/exporter"
)

// ossAccelOutstandingWorkSnapshot holds the last value each contributing
// sweep reported for one vol. Each field is updated independently, at
// whatever cadence its own sweep runs on — this struct is a cache of
// "as of the last time each sweep happened", not a live queue.
type ossAccelOutstandingWorkSnapshot struct {
	flushPolicyCandidates int
	auditDangling         int
	trashPending          int
}

// ossAccelUpdateOutstandingWork applies update to vol's cached snapshot
// (creating one on first use), then re-exports the combined gauge. Called
// from all three contributing sweeps (runOssAccelFlushPolicyForVol,
// runOssAccelAuditForVol, runOssAccelTrashPurgeForVol), each touching only
// its own field — the other two retain whatever they were last set to.
func (l *LcNode) ossAccelUpdateOutstandingWork(vol string, update func(*ossAccelOutstandingWorkSnapshot)) {
	l.ossAccelOutstandingWorkMu.Lock()
	snap, ok := l.ossAccelOutstandingWork[vol]
	if !ok {
		snap = &ossAccelOutstandingWorkSnapshot{}
		l.ossAccelOutstandingWork[vol] = snap
	}
	update(snap)
	total := snap.flushPolicyCandidates + snap.auditDangling + snap.trashPending
	l.ossAccelOutstandingWorkMu.Unlock()

	exporter.NewGauge("oss_accel_outstanding_work_estimate_lcnode").SetWithLabels(float64(total), map[string]string{exporter.Vol: vol})
}
