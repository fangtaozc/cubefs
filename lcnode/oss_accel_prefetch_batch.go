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

// 差距分析续(对照 AFM mmafmctl prefetch / 阿里云 EFC job化warmup): the single-
// path/prefix /ossAccelPrefetch (oss_accel_prefetch.go) is synchronous — the
// caller blocks until the whole walk-and-download finishes and gets back one
// summary line, with no way to check progress mid-flight or submit a
// specific list of paths (as opposed to "everything under this prefix").
// This file adds that: submit a path list, get a task ID back immediately,
// poll it for progress. Deliberately lcnode-local (see LcNode.ossAccelBatch-
// Tasks doc comment in server.go) — no master involvement, no raft.
package lcnode

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/syncnode/backend/s3"
	"github.com/cubefs/cubefs/util/log"
	"github.com/google/uuid"
)

// ossAccelBatchPrefetchMaxPaths caps a single submission to keep the task
// registry and the in-memory Paths slice bounded — a caller with a larger
// list submits multiple batches rather than this endpoint silently paging
// internally. Not tuned against any measured limit, just a sane stop against
// an accidental multi-million-entry submission; raise it if a real workload
// needs more (see plan doc — YAGNI on auto-sharding).
const ossAccelBatchPrefetchMaxPaths = 1000

const (
	ossAccelBatchStatusRunning = "running"
	ossAccelBatchStatusDone    = "done"
	ossAccelBatchStatusFailed  = "failed"
)

// ossAccelBatchPrefetchTask tracks one submitted batch. Progress fields are
// mutated by the background goroutine running the batch and read by
// concurrent status-query requests — hence the mutex; StartedAt/FinishedAt
// use zero-value time.Time to mean "not yet".
type ossAccelBatchPrefetchTask struct {
	ID   string
	Vol  string
	mu   sync.Mutex
	done int
	hot  int
	errs int
	// stoppedForCapacity is set once and never incremented further — it's
	// the count of paths that were never attempted because the volume
	// crossed maxUsageRatio partway through, not a per-path counter.
	stoppedForCapacity int
	total              int
	status             string
	startedAt          time.Time
	finishedAt         time.Time
}

// ossAccelBatchPrefetchStatus is the JSON shape returned by the status
// endpoint — a snapshot, not a live reference, so the caller can't observe
// half-updated state across the task's mutex.
type ossAccelBatchPrefetchStatus struct {
	TaskID             string `json:"taskId"`
	Vol                string `json:"vol"`
	Status             string `json:"status"`
	Total              int    `json:"total"`
	Done               int    `json:"done"`
	AlreadyHot         int    `json:"alreadyHot"`
	Errors             int    `json:"errors"`
	StoppedForCapacity int    `json:"stoppedForCapacity"`
	StartedAt          string `json:"startedAt,omitempty"`
	FinishedAt         string `json:"finishedAt,omitempty"`
}

func (t *ossAccelBatchPrefetchTask) snapshot() ossAccelBatchPrefetchStatus {
	t.mu.Lock()
	defer t.mu.Unlock()
	s := ossAccelBatchPrefetchStatus{
		TaskID:             t.ID,
		Vol:                t.Vol,
		Status:             t.status,
		Total:              t.total,
		Done:               t.done,
		AlreadyHot:         t.hot,
		Errors:             t.errs,
		StoppedForCapacity: t.stoppedForCapacity,
	}
	if !t.startedAt.IsZero() {
		s.StartedAt = t.startedAt.Format(time.RFC3339)
	}
	if !t.finishedAt.IsZero() {
		s.FinishedAt = t.finishedAt.Format(time.RFC3339)
	}
	return s
}

// httpServiceOssAccelPrefetchBatch handles
// GET /ossAccelPrefetchBatch?vol=&paths=<comma-separated>&sc=&vsc=&asc=[&maxUsageRatio=0.9]
// Registers the task and returns its ID immediately; the actual recall work
// runs in a background goroutine (runOssAccelPrefetchBatch below).
func (l *LcNode) httpServiceOssAccelPrefetchBatch(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, fmt.Sprintf("ParseForm err: %v", err), http.StatusBadRequest)
		return
	}
	vol := r.FormValue("vol")
	if vol == "" {
		http.Error(w, "missing required form value: vol", http.StatusBadRequest)
		return
	}
	rawPaths := r.FormValue("paths")
	if rawPaths == "" {
		http.Error(w, "missing required form value: paths", http.StatusBadRequest)
		return
	}
	var paths []string
	for _, p := range strings.Split(rawPaths, ",") {
		if p == "" {
			continue
		}
		paths = append(paths, p)
	}
	if len(paths) == 0 {
		http.Error(w, "paths must contain at least one non-empty entry", http.StatusBadRequest)
		return
	}
	if len(paths) > ossAccelBatchPrefetchMaxPaths {
		http.Error(w, fmt.Sprintf("paths has %v entries, exceeds max %v per submission — split into multiple batches", len(paths), ossAccelBatchPrefetchMaxPaths), http.StatusBadRequest)
		return
	}
	_, vsc, asc, err := parseStorageClassForm(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	maxUsageRatio := ossAccelPrefetchDefaultMaxUsageRatio
	if raw := r.FormValue("maxUsageRatio"); raw != "" {
		parsed, perr := strconv.ParseFloat(raw, 64)
		if perr != nil {
			http.Error(w, fmt.Sprintf("ParseFloat maxUsageRatio err: %v", perr), http.StatusBadRequest)
			return
		}
		maxUsageRatio = parsed
	}

	task := &ossAccelBatchPrefetchTask{
		ID:        uuid.New().String(),
		Vol:       vol,
		total:     len(paths),
		status:    ossAccelBatchStatusRunning,
		startedAt: time.Now(),
	}
	l.ossAccelBatchTasksMu.Lock()
	l.ossAccelBatchTasks[task.ID] = task
	l.ossAccelBatchTasksMu.Unlock()

	go l.runOssAccelPrefetchBatch(task, paths, vsc, asc, maxUsageRatio)

	fmt.Fprintf(w, "ok: taskId=%v vol=%v total=%v\n", task.ID, vol, len(paths))
}

// httpServiceOssAccelPrefetchBatchStatus handles
// GET /ossAccelPrefetchBatchStatus?taskId=
func (l *LcNode) httpServiceOssAccelPrefetchBatchStatus(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, fmt.Sprintf("ParseForm err: %v", err), http.StatusBadRequest)
		return
	}
	taskID := r.FormValue("taskId")
	if taskID == "" {
		http.Error(w, "missing required form value: taskId", http.StatusBadRequest)
		return
	}
	l.ossAccelBatchTasksMu.Lock()
	task, ok := l.ossAccelBatchTasks[taskID]
	l.ossAccelBatchTasksMu.Unlock()
	if !ok {
		http.Error(w, fmt.Sprintf("no batch prefetch task found for taskId %q — it may have been submitted to a different lcnode, or this lcnode restarted since submission", taskID), http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(task.snapshot()); err != nil {
		log.LogErrorf("httpServiceOssAccelPrefetchBatchStatus: encode err: %v", err)
	}
}

// runOssAccelPrefetchBatch is the background worker for one submitted batch.
// Unlike runOssAccelPrefetchForVol's single up-front capacity check (safe
// there because that call is short-lived and usage is monotonically
// non-decreasing across it), a batch can run for a long time — usage is
// re-checked before EACH path so a batch that starts under the watermark but
// crosses it partway through stops cleanly instead of overshooting on stale
// information.
func (l *LcNode) runOssAccelPrefetchBatch(task *ossAccelBatchPrefetchTask, paths []string, vsc uint32, asc []uint32, maxUsageRatio float64) {
	var err error
	defer ossAccelObserve("prefetchBatch", task.Vol, &err)()
	defer func() {
		task.mu.Lock()
		task.finishedAt = time.Now()
		if err != nil {
			task.status = ossAccelBatchStatusFailed
		} else {
			task.status = ossAccelBatchStatusDone
		}
		task.mu.Unlock()
	}()

	metaWrapper, extentClient, berr := l.buildVolClients(task.Vol, vsc, asc)
	if berr != nil {
		err = berr
		return
	}
	defer metaWrapper.Close()
	defer extentClient.Close()

	s3Cfg, cerr := loadOssAccelS3Config(metaWrapper, task.Vol)
	if cerr != nil {
		err = cerr
		return
	}
	s3Backend, nerr := s3.New(s3Cfg)
	if nerr != nil {
		err = fmt.Errorf("s3 backend init err: %v", nerr)
		return
	}
	defer s3Backend.Close()

	for _, path := range paths {
		if usageRatio := ossAccelVolUsageRatio(metaWrapper); usageRatio >= maxUsageRatio {
			log.LogWarnf("runOssAccelPrefetchBatch: taskId(%v) vol(%v) usageRatio(%.4f) >= maxUsageRatio(%.4f) — stopping, %v path(s) not attempted",
				task.ID, task.Vol, usageRatio, maxUsageRatio, len(paths))
			task.mu.Lock()
			task.stoppedForCapacity = len(paths) - task.done - task.hot - task.errs
			task.mu.Unlock()
			break
		}

		cand, alreadyHot, dangling, rerr := ossAccelResolvePrefetchCandidate(metaWrapper, path)
		if rerr != nil {
			log.LogWarnf("runOssAccelPrefetchBatch: taskId(%v) vol(%v) path(%v) resolve err: %v", task.ID, task.Vol, path, rerr)
			task.mu.Lock()
			task.errs++
			task.mu.Unlock()
			continue
		}
		if alreadyHot {
			task.mu.Lock()
			task.hot++
			task.mu.Unlock()
			continue
		}
		if dangling {
			task.mu.Lock()
			task.errs++
			task.mu.Unlock()
			continue
		}

		recallKey := task.Vol + "/" + strconv.FormatUint(cand.ino, 10)
		if aerr := l.ossAccelRecallLimit.Acquire(recallKey, 1); aerr != nil {
			// Someone else (a real client read, or an overlapping prefetch
			// call) is already recalling this exact inode — not a failure
			// for THIS batch, the other caller's work covers it. Counted as
			// done, not errs: the file WILL be hot by the time whichever
			// caller wins finishes, matching this batch's intent.
			log.LogInfof("runOssAccelPrefetchBatch: taskId(%v) vol(%v) ino(%v) already being recalled elsewhere — counting as done, not an error", task.ID, task.Vol, cand.ino)
			task.mu.Lock()
			task.done++
			task.mu.Unlock()
			continue
		}
		_, _, _, rcErr := l.runOssAccelRecallForInode(metaWrapper, extentClient, s3Backend, task.Vol, cand.path, cand.ino, cand.size, vsc, cand.checksum)
		l.ossAccelRecallLimit.Release(recallKey)
		task.mu.Lock()
		if rcErr != nil {
			log.LogWarnf("runOssAccelPrefetchBatch: taskId(%v) vol(%v) path(%v) ino(%v) prefetch err: %v", task.ID, task.Vol, cand.path, cand.ino, rcErr)
			task.errs++
		} else {
			task.done++
		}
		task.mu.Unlock()
	}
}
