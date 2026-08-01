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

// 差距分析续三(聚合并发无上限): ossAccelRecallLimit (util/concurrent.
// KeyConcurrentLimit) dedupes concurrent recall attempts on the SAME inode —
// it was never a total-count cap. Nothing bounded how many DIFFERENT files'
// recalls this lcnode process runs at once across the manual recall
// endpoint (oss_accel.go), the eager prefetch sweep (oss_accel_prefetch.go),
// and batch prefetch tasks (oss_accel_prefetch_batch.go) — the risk this
// creates became concrete once batch prefetch shipped (a caller can now
// submit hundreds of paths in one request, on top of whatever real client
// reads and other in-flight batches are already doing).
//
// This file centralizes the two-layer Acquire/Release all three callers now
// share, so the ordering (dedupe lock first, then the total-count budget)
// and the release ordering (reverse) live in one place instead of being
// hand-copied three times.
package lcnode

import (
	"errors"
	"strconv"

	"github.com/cubefs/cubefs/blobstore/util/limit"
	"github.com/cubefs/cubefs/util/log"
)

// ossAccelRecallSlotsExhaustedErr wraps limit.ErrLimited with a message
// callers can surface directly — the underlying error alone ("limit
// exceeded") doesn't say what was exhausted or that retrying later is the
// right response.
type ossAccelRecallSlotsExhaustedErr struct {
	vol string
	ino uint64
}

func (e *ossAccelRecallSlotsExhaustedErr) Error() string {
	return "vol(" + e.vol + ") ino(" + strconv.FormatUint(e.ino, 10) + "): this lcnode's total in-flight recall budget is exhausted; retry later"
}

func (e *ossAccelRecallSlotsExhaustedErr) Unwrap() error { return limit.ErrLimited }

// ossAccelAcquireRecallSlots acquires BOTH layers for (vol, ino): the
// existing per-inode dedupe lock (l.ossAccelRecallLimit), then the new
// process-wide total budget (l.ossAccelInflightRecallLimit). The dedupe
// check runs first because it's cheaper and more common to fail (a
// concurrent recall on the SAME file is the everyday case
// waitForConcurrentRecallWinner already handles) — no point spending a
// budget token on a request the dedupe layer would reject anyway.
//
// Returns the recallKey (so callers don't have to recompute it for
// Release) and one of:
//   - (key, false, false, nil): dedupe lock lost — SOMEONE ELSE on this
//     lcnode is already recalling this exact inode. Not this function's
//     concern how the caller reacts (join the winner via
//     waitForConcurrentRecallWinner, or just skip it) — same as before this
//     file existed, only now expressed as a bool instead of the caller
//     checking the error itself.
//   - (key, true, false, err): dedupe lock WON but the total-inflight
//     budget is exhausted — err is *ossAccelRecallSlotsExhaustedErr. The
//     dedupe lock has already been released by the time this returns (see
//     below), so the caller must NOT call Release for this attempt.
//   - (key, true, true, nil): both acquired — proceed, and Release when done.
func (l *LcNode) ossAccelAcquireRecallSlots(vol string, ino uint64) (recallKey string, dedupeWon, slotAcquired bool, err error) {
	recallKey = vol + "/" + strconv.FormatUint(ino, 10)
	if aerr := l.ossAccelRecallLimit.Acquire(recallKey, 1); aerr != nil {
		return recallKey, false, false, nil
	}
	if serr := l.ossAccelInflightRecallLimit.Acquire(); serr != nil {
		// Total budget exhausted — release the dedupe lock we just won
		// rather than leaving it held for an attempt that isn't actually
		// going to run. This is temporary backpressure (some other
		// in-flight recall will finish and free a slot), not a permanent
		// rejection like the dead-lettering elsewhere in oss-accel — the
		// caller is expected to retry, not give up on this file.
		l.ossAccelRecallLimit.Release(recallKey)
		if !errors.Is(serr, limit.ErrLimited) {
			// count.Limiter.Acquire only ever returns limit.ErrLimited in
			// its non-blocking form (see blobstore/util/limit/count.go) —
			// this branch exists so a future swap to a different Limiter
			// implementation doesn't silently get misreported as capacity
			// exhaustion.
			log.LogWarnf("ossAccelAcquireRecallSlots: vol(%v) ino(%v) unexpected inflight-limit error: %v", vol, ino, serr)
		}
		return recallKey, true, false, &ossAccelRecallSlotsExhaustedErr{vol: vol, ino: ino}
	}
	return recallKey, true, true, nil
}

// ossAccelReleaseRecallSlots releases both layers in reverse acquire order.
// Only call this when ossAccelAcquireRecallSlots returned slotAcquired=true
// — the exhausted-budget path above already released the dedupe lock itself.
func (l *LcNode) ossAccelReleaseRecallSlots(recallKey string) {
	l.ossAccelInflightRecallLimit.Release()
	l.ossAccelRecallLimit.Release(recallKey)
}
