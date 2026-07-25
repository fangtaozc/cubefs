// Copyright 2023 The CubeFS Authors.
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

package lcnode

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/auditlog"
	"github.com/cubefs/cubefs/util/log"
)

func (l *LcNode) opMasterHeartbeat(conn net.Conn, p *proto.Packet, remoteAddr string) (err error) {
	data := p.Data
	responseAckOKToMaster(conn, p)

	var (
		req  = &proto.HeartBeatRequest{}
		resp = &proto.LcNodeHeartbeatResponse{
			LcScanningTasks:       make(map[string]*proto.LcNodeRuleTaskResponse),
			SnapshotScanningTasks: make(map[string]*proto.SnapshotVerDelTaskResponse),
		}
		adminTask = &proto.AdminTask{
			Request: req,
		}
	)

	go func() {
		start := time.Now()
		decode := json.NewDecoder(bytes.NewBuffer(data))
		decode.UseNumber()
		if err = decode.Decode(adminTask); err != nil {
			resp.Status = proto.TaskFailed
			resp.Result = fmt.Sprintf("lcnode(%v) heartbeat decode err(%v)", l.localServerAddr, err.Error())
			goto end
		}

		l.scannerMutex.RLock()
		for _, scanner := range l.lcScanners {
			result := &proto.LcNodeRuleTaskResponse{
				ID:        scanner.ID,
				LcNode:    l.localServerAddr,
				StartTime: &scanner.now,
				Volume:    scanner.Volume,
				RcvStop:   scanner.receiveStop,
				Rule:      scanner.rule,
				LcNodeRuleTaskStatistics: proto.LcNodeRuleTaskStatistics{
					TotalFileScannedNum:      atomic.LoadInt64(&scanner.currentStat.TotalFileScannedNum),
					TotalFileExpiredNum:      atomic.LoadInt64(&scanner.currentStat.TotalFileExpiredNum),
					TotalDirScannedNum:       atomic.LoadInt64(&scanner.currentStat.TotalDirScannedNum),
					ExpiredDeleteNum:         atomic.LoadInt64(&scanner.currentStat.ExpiredDeleteNum),
					ExpiredMToHddNum:         atomic.LoadInt64(&scanner.currentStat.ExpiredMToHddNum),
					ExpiredMToBlobstoreNum:   atomic.LoadInt64(&scanner.currentStat.ExpiredMToBlobstoreNum),
					ExpiredMToHddBytes:       atomic.LoadInt64(&scanner.currentStat.ExpiredMToHddBytes),
					ExpiredMToBlobstoreBytes: atomic.LoadInt64(&scanner.currentStat.ExpiredMToBlobstoreBytes),
					ExpiredSkipNum:           atomic.LoadInt64(&scanner.currentStat.ExpiredSkipNum),
					ErrorDeleteNum:           atomic.LoadInt64(&scanner.currentStat.ErrorDeleteNum),
					ErrorMToHddNum:           atomic.LoadInt64(&scanner.currentStat.ErrorMToHddNum),
					ErrorMToBlobstoreNum:     atomic.LoadInt64(&scanner.currentStat.ErrorMToBlobstoreNum),
					ErrorReadDirNum:          atomic.LoadInt64(&scanner.currentStat.ErrorReadDirNum),
				},
			}
			resp.LcScanningTasks[scanner.ID] = result
		}
		for _, scanner := range l.snapshotScanners {
			info := &proto.SnapshotVerDelTaskResponse{
				ID:                 scanner.ID,
				LcNode:             l.localServerAddr,
				SnapshotVerDelTask: scanner.verDelReq.Task,
				SnapshotStatistics: proto.SnapshotStatistics{
					VolName:         scanner.Volume,
					VerSeq:          scanner.getTaskVerSeq(),
					TotalInodeNum:   atomic.LoadInt64(&scanner.currentStat.TotalInodeNum),
					FileNum:         atomic.LoadInt64(&scanner.currentStat.FileNum),
					DirNum:          atomic.LoadInt64(&scanner.currentStat.DirNum),
					ErrorSkippedNum: atomic.LoadInt64(&scanner.currentStat.ErrorSkippedNum),
				},
			}
			resp.SnapshotScanningTasks[scanner.ID] = info
		}
		l.scannerMutex.RUnlock()

		resp.LcTaskCountLimit = lcNodeTaskCountLimit
		resp.Status = proto.TaskSucceeds

	end:
		adminTask.Response = resp
		l.respondToMaster(adminTask)
		msg := fmt.Sprintf("from(%v), adminTask(%+v), resp(%+v), %v", remoteAddr, adminTask, resp, time.Since(start).String())
		log.LogInfof("MasterHeartbeat %v ", msg)
		auditlog.LogMasterOp("MasterHeartbeat", msg, err)
	}()

	l.lastHeartbeat = time.Now()
	log.LogDebugf("lastHeartbeat: %v", l.lastHeartbeat)
	return
}

func (l *LcNode) opLcScan(conn net.Conn, p *proto.Packet) (err error) {
	data := p.Data

	responseAckOKToMaster(conn, p)

	go func() {
		var (
			req       = &proto.LcNodeRuleTaskRequest{}
			resp      = &proto.LcNodeRuleTaskResponse{}
			adminTask = &proto.AdminTask{
				Request: req,
			}
		)

		decoder := json.NewDecoder(bytes.NewBuffer(data))
		decoder.UseNumber()
		if err = decoder.Decode(adminTask); err != nil {
			resp.LcNode = l.localServerAddr
			resp.Status = proto.TaskFailed
			resp.Done = true
			resp.StartErr = err.Error()
			adminTask.Response = resp
			l.respondToMaster(adminTask)
			return
		}

		l.startLcScan(adminTask)
		l.respondToMaster(adminTask)
	}()

	return
}

func (l *LcNode) respondToMaster(task *proto.AdminTask) {
	// handle panic
	defer func() {
		if r := recover(); r != nil {
			log.LogErrorf("respondToMaster err: %v", r)
		}
	}()
	if err := l.mc.NodeAPI().ResponseLcNodeTask(task); err != nil {
		log.LogErrorf("respondToMaster err: %v, task: %v", err, task)
	}
}

func (l *LcNode) opSnapshotVerDel(conn net.Conn, p *proto.Packet) (err error) {
	data := p.Data

	responseAckOKToMaster(conn, p)

	go func() {
		var (
			req       = &proto.SnapshotVerDelTaskRequest{}
			resp      = &proto.SnapshotVerDelTaskResponse{}
			adminTask = &proto.AdminTask{
				Request: req,
			}
		)

		decoder := json.NewDecoder(bytes.NewBuffer(data))
		decoder.UseNumber()
		if err = decoder.Decode(adminTask); err != nil {
			resp.Status = proto.TaskFailed
			resp.Result = err.Error()
			adminTask.Response = resp
			l.respondToMaster(adminTask)
			return
		}

		l.startSnapshotScan(adminTask)
		l.respondToMaster(adminTask)
	}()

	return
}

// opOssAccelChangelogSync handles OpLcNodeOssAccelChangelogSync — the
// master-scheduled AdminTask counterpart to the manual
// GET /ossAccelChangelogSync debug endpoint (M2 production automation,
// see docs/plan/cubefs-oss-accel-m2-design.md and
// master/oss_accel_changelog_rule_manager.go). Unlike opLcScan's
// long-running, cancellable LcScanner, a changelog sync task completes in a
// single call (one Range GET + a handful of Create_ll calls) — no scanner
// registry needed, just run it synchronously in the ack goroutine and
// respond, mirroring opLcScan's outer envelope only.
func (l *LcNode) opOssAccelChangelogSync(conn net.Conn, p *proto.Packet) (err error) {
	data := p.Data

	responseAckOKToMaster(conn, p)

	go func() {
		var (
			req       = &proto.OSSAccelChangelogSyncTaskRequest{}
			resp      = &proto.OSSAccelChangelogSyncTaskResponse{}
			adminTask = &proto.AdminTask{Request: req}
		)

		decoder := json.NewDecoder(bytes.NewBuffer(data))
		decoder.UseNumber()
		if derr := decoder.Decode(adminTask); derr != nil {
			resp.LcNode = l.localServerAddr
			resp.Done = true
			resp.StartErr = derr.Error()
			adminTask.Response = resp
			l.respondToMaster(adminTask)
			return
		}
		request := adminTask.Request.(*proto.OSSAccelChangelogSyncTaskRequest)

		start := time.Now()
		resp.VolName = request.VolName
		resp.LcNode = l.localServerAddr
		resp.StartTime = &start

		processed, skipped, failed, _, cursor, runErr := l.runOssAccelChangelogSync(request.VolName, request.Prefix, request.ChangelogKey, request.SkipAfterFailures, request.ConsecutiveFailures)
		end := time.Now()
		resp.EndTime = &end
		resp.Done = true
		resp.Processed = processed
		resp.Skipped = skipped
		resp.Failed = failed
		resp.Cursor = cursor
		if runErr != nil {
			resp.StartErr = runErr.Error()
		}

		// Placeholder TTL sweep (M2 收尾阶段 N) piggybacks on this same
		// dispatch rather than getting its own lcnode ticker — see
		// runOssAccelPlaceholderSweep's doc comment. Runs regardless of
		// runErr above (an unrelated changelog-sync failure shouldn't block
		// reclaiming already-expired placeholders from earlier syncs).
		if swept, sweepErr := l.runOssAccelPlaceholderSweep(request.VolName, request.PlaceholderTTLSeconds); sweepErr != nil {
			log.LogErrorf("opOssAccelChangelogSync: vol(%v) placeholder sweep err: %v", request.VolName, sweepErr)
		} else {
			resp.Swept = swept
		}

		adminTask.Response = resp
		l.respondToMaster(adminTask)
	}()

	return
}

// opOssAccelEvict handles OpLcNodeOssAccelEvict — the M3 water-level
// coldest-first eviction sweep AdminTask (see
// master/oss_accel_eviction_rule_manager.go and
// lcnode/oss_accel_evict.go runOssAccelEvictionSweep). Same envelope
// shape as opOssAccelChangelogSync: synchronous, single-goroutine, no
// scanner registry (a sweep completes in one call, no need for opLcScan's
// long-running task tracking).
func (l *LcNode) opOssAccelEvict(conn net.Conn, p *proto.Packet) (err error) {
	data := p.Data

	responseAckOKToMaster(conn, p)

	go func() {
		var (
			req       = &proto.OSSAccelEvictionTaskRequest{}
			resp      = &proto.OSSAccelEvictionTaskResponse{}
			adminTask = &proto.AdminTask{Request: req}
		)

		decoder := json.NewDecoder(bytes.NewBuffer(data))
		decoder.UseNumber()
		if derr := decoder.Decode(adminTask); derr != nil {
			resp.LcNode = l.localServerAddr
			resp.Done = true
			resp.StartErr = derr.Error()
			adminTask.Response = resp
			l.respondToMaster(adminTask)
			return
		}
		request := adminTask.Request.(*proto.OSSAccelEvictionTaskRequest)

		start := time.Now()
		resp.VolName = request.VolName
		resp.LcNode = l.localServerAddr
		resp.StartTime = &start

		considered, evicted, usageRatioAfter, runErr := l.runOssAccelEvictionSweep(request.VolName, request.LowWatermarkRatio)
		end := time.Now()
		resp.EndTime = &end
		resp.Done = true
		resp.CandidatesConsidered = considered
		resp.Evicted = evicted
		resp.UsageRatioAfter = usageRatioAfter
		if runErr != nil {
			resp.StartErr = runErr.Error()
		}

		adminTask.Response = resp
		l.respondToMaster(adminTask)
	}()

	return
}

// opOssAccelAudit handles OpLcNodeOssAccelAudit — 系统层面收尾 (自动化程度
// 不均): the master-scheduled AdminTask counterpart to the manual
// GET /ossAccelAudit endpoint (see master/oss_accel_audit_rule_manager.go).
// Same envelope shape as opOssAccelEvict/opOssAccelChangelogSync.
func (l *LcNode) opOssAccelAudit(conn net.Conn, p *proto.Packet) (err error) {
	data := p.Data

	responseAckOKToMaster(conn, p)

	go func() {
		var (
			req       = &proto.OSSAccelAuditTaskRequest{}
			resp      = &proto.OSSAccelAuditTaskResponse{}
			adminTask = &proto.AdminTask{Request: req}
		)

		decoder := json.NewDecoder(bytes.NewBuffer(data))
		decoder.UseNumber()
		if derr := decoder.Decode(adminTask); derr != nil {
			resp.LcNode = l.localServerAddr
			resp.Done = true
			resp.StartErr = derr.Error()
			adminTask.Response = resp
			l.respondToMaster(adminTask)
			return
		}
		request := adminTask.Request.(*proto.OSSAccelAuditTaskRequest)

		start := time.Now()
		resp.VolName = request.VolName
		resp.LcNode = l.localServerAddr
		resp.StartTime = &start

		result, runErr := l.runOssAccelAuditForVol(request.VolName, request.Prefix, uint64(request.OrphanGraceHours))
		end := time.Now()
		resp.EndTime = &end
		resp.Done = true
		resp.Dangling = len(result.DanglingKeys)
		resp.DanglingUnmarked = len(result.DanglingUnmarkedKeys)
		resp.Orphans = len(result.OrphanCandidateKeys)
		resp.Quarantined = len(result.QuarantinedKeys)
		resp.OrphanRefused = len(result.OrphanRefusedKeys)
		resp.DriftDetected = len(result.DriftDetectedKeys)
		resp.Relocated = len(result.RelocatedKeys)
		resp.DriftConflicts = len(result.DriftConflictKeys)
		resp.DriftRefused = len(result.DriftRefusedKeys)
		if runErr != nil {
			resp.StartErr = runErr.Error()
		}

		adminTask.Response = resp
		l.respondToMaster(adminTask)
	}()

	return
}

// opOssAccelTrashPurge handles OpLcNodeOssAccelTrashPurge — 系统层面收尾:
// the master-scheduled AdminTask counterpart to the manual
// GET /ossAccelTrashPurge endpoint (see
// master/oss_accel_trash_purge_rule_manager.go). Same envelope shape as
// opOssAccelAudit.
func (l *LcNode) opOssAccelTrashPurge(conn net.Conn, p *proto.Packet) (err error) {
	data := p.Data

	responseAckOKToMaster(conn, p)

	go func() {
		var (
			req       = &proto.OSSAccelTrashPurgeTaskRequest{}
			resp      = &proto.OSSAccelTrashPurgeTaskResponse{}
			adminTask = &proto.AdminTask{Request: req}
		)

		decoder := json.NewDecoder(bytes.NewBuffer(data))
		decoder.UseNumber()
		if derr := decoder.Decode(adminTask); derr != nil {
			resp.LcNode = l.localServerAddr
			resp.Done = true
			resp.StartErr = derr.Error()
			adminTask.Response = resp
			l.respondToMaster(adminTask)
			return
		}
		request := adminTask.Request.(*proto.OSSAccelTrashPurgeTaskRequest)

		start := time.Now()
		resp.VolName = request.VolName
		resp.LcNode = l.localServerAddr
		resp.StartTime = &start

		purged, refused, runErr := l.runOssAccelTrashPurgeForVol(request.VolName, request.Prefix, uint64(request.RetentionHours))
		end := time.Now()
		resp.EndTime = &end
		resp.Done = true
		resp.Purged = len(purged)
		resp.Refused = len(refused)
		if runErr != nil {
			resp.StartErr = runErr.Error()
		}

		adminTask.Response = resp
		l.respondToMaster(adminTask)
	}()

	return
}

// opOssAccelFlushPolicy handles OpLcNodeOssAccelFlushPolicy — 系统层面收尾续
// (补1+3): the master-scheduled AdminTask counterpart to the age-triggered
// auto flush+commit-cold sweep (see master/oss_accel_flush_policy_rule_manager.go).
// Same envelope shape as opOssAccelAudit.
func (l *LcNode) opOssAccelFlushPolicy(conn net.Conn, p *proto.Packet) (err error) {
	data := p.Data

	responseAckOKToMaster(conn, p)

	go func() {
		var (
			req       = &proto.OSSAccelFlushPolicyTaskRequest{}
			resp      = &proto.OSSAccelFlushPolicyTaskResponse{}
			adminTask = &proto.AdminTask{Request: req}
		)

		decoder := json.NewDecoder(bytes.NewBuffer(data))
		decoder.UseNumber()
		if derr := decoder.Decode(adminTask); derr != nil {
			resp.LcNode = l.localServerAddr
			resp.Done = true
			resp.StartErr = derr.Error()
			adminTask.Response = resp
			l.respondToMaster(adminTask)
			return
		}
		request := adminTask.Request.(*proto.OSSAccelFlushPolicyTaskRequest)

		start := time.Now()
		resp.VolName = request.VolName
		resp.LcNode = l.localServerAddr
		resp.StartTime = &start

		scanned, flushed, skipped, errCount, runErr := l.runOssAccelFlushPolicyForVol(request.VolName, request.Prefix, request.MinIdleHours, request.MinSizeBytes)
		end := time.Now()
		resp.EndTime = &end
		resp.Done = true
		resp.Scanned = scanned
		resp.Flushed = flushed
		resp.Skipped = skipped
		resp.Errors = errCount
		if runErr != nil {
			resp.StartErr = runErr.Error()
		}

		adminTask.Response = resp
		l.respondToMaster(adminTask)
	}()

	return
}

// opOssAccelBucketScan handles OpLcNodeOssAccelBucketScan — the master-scheduled
// AdminTask counterpart to the manual /ossAccelRegister prefix mode (see
// master/oss_accel_bucket_scan_rule_manager.go). Same envelope shape as
// opOssAccelAudit; shares runOssAccelRegisterForVol with the manual HTTP path.
func (l *LcNode) opOssAccelBucketScan(conn net.Conn, p *proto.Packet) (err error) {
	data := p.Data

	responseAckOKToMaster(conn, p)

	go func() {
		var (
			req       = &proto.OSSAccelBucketScanTaskRequest{}
			resp      = &proto.OSSAccelBucketScanTaskResponse{}
			adminTask = &proto.AdminTask{Request: req}
		)

		decoder := json.NewDecoder(bytes.NewBuffer(data))
		decoder.UseNumber()
		if derr := decoder.Decode(adminTask); derr != nil {
			resp.LcNode = l.localServerAddr
			resp.Done = true
			resp.StartErr = derr.Error()
			adminTask.Response = resp
			l.respondToMaster(adminTask)
			return
		}
		request := adminTask.Request.(*proto.OSSAccelBucketScanTaskRequest)

		start := time.Now()
		resp.VolName = request.VolName
		resp.LcNode = l.localServerAddr
		resp.StartTime = &start

		materialized, skipped, errCount, runErr := l.runOssAccelRegisterForVol(request.VolName, nil, request.Prefix)
		end := time.Now()
		resp.EndTime = &end
		resp.Done = true
		resp.Materialized = materialized
		resp.Skipped = skipped
		resp.Errors = errCount
		if runErr != nil {
			resp.StartErr = runErr.Error()
		}

		adminTask.Response = resp
		l.respondToMaster(adminTask)
	}()

	return
}

// opOssAccelIntegrity handles OpLcNodeOssAccelIntegrity — 系统层面收尾续
// (补1+3): the master-scheduled AdminTask counterpart to the cold-tier
// integrity verification sweep (see master/oss_accel_integrity_rule_manager.go).
// Same envelope shape as opOssAccelAudit.
func (l *LcNode) opOssAccelIntegrity(conn net.Conn, p *proto.Packet) (err error) {
	data := p.Data

	responseAckOKToMaster(conn, p)

	go func() {
		var (
			req       = &proto.OSSAccelIntegrityTaskRequest{}
			resp      = &proto.OSSAccelIntegrityTaskResponse{}
			adminTask = &proto.AdminTask{Request: req}
		)

		decoder := json.NewDecoder(bytes.NewBuffer(data))
		decoder.UseNumber()
		if derr := decoder.Decode(adminTask); derr != nil {
			resp.LcNode = l.localServerAddr
			resp.Done = true
			resp.StartErr = derr.Error()
			adminTask.Response = resp
			l.respondToMaster(adminTask)
			return
		}
		request := adminTask.Request.(*proto.OSSAccelIntegrityTaskRequest)

		start := time.Now()
		resp.VolName = request.VolName
		resp.LcNode = l.localServerAddr
		resp.StartTime = &start

		result, runErr := l.runOssAccelIntegrityForVol(request.VolName, request.Prefix, request.FullSampleCount)
		end := time.Now()
		resp.EndTime = &end
		resp.Done = true
		resp.CheapChecked = result.CheapChecked
		resp.FullChecked = result.FullChecked
		resp.Mismatches = result.Mismatches
		resp.MismatchesUnmarked = result.MismatchesUnmarked
		if runErr != nil {
			resp.StartErr = runErr.Error()
		}

		adminTask.Response = resp
		l.respondToMaster(adminTask)
	}()

	return
}

func responseAckOKToMaster(conn net.Conn, p *proto.Packet) {
	go func() {
		p.PacketOkReply()
		if err := p.WriteToConn(conn); err != nil {
			log.LogErrorf("ack master response: %s", err.Error())
		}
	}()
}
