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

package lcnode

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
)

// 系统层面收尾续(补1+3) — age-triggered auto flush+commit-cold sweep.
// Dispatched via OpLcNodeOssAccelFlushPolicy (opOssAccelFlushPolicy,
// lcnode/lc_op.go), master-scheduled by OSSAccelFlushPolicyRuleManager on a
// plain elapsed-time poll (master/oss_accel_flush_policy_rule_manager.go).
//
// Candidate = XAttrKeyOSSAccelS3Key ABSENT (never tiered out via oss-accel
// before — this is what makes this sweep orthogonal to
// runOssAccelEvictionSweep, which only re-cools files that already have an
// s3key) && StorageClass is a replica class (resident in the hot tier) &&
// not pinned && Size >= MinSizeBytes && idle (ModifyTime) for at least
// MinIdleHours.
//
// Idle signal is ModifyTime, not AccessTime: metanode's native AccessTime
// requires a per-volume opt-in (EnablePersistAccessTime) that isn't
// guaranteed on every volume — the same reason
// XAttrKeyOSSAccelLastRecallTime exists instead of relying on AccessTime for
// eviction ranking (see that constant's doc comment). ModifyTime is always
// populated, at the cost of only sensing "last WRITTEN," not "last READ": a
// file that's read frequently but never modified will still be flushed —
// harmless (the existing cold-read gate transparently recalls it on the next
// read), just an avoidable extra recall round-trip. Known limitation,
// documented rather than solved this round.
//
// A matching candidate is flushed AND immediately committed cold in one
// step (runOssAccelFlushForVol then runOssAccelCommitCold) — this sweep's
// whole point is to actually free local disk space for old files, not just
// stage an S3 backup while the hot copy lingers forever on a
// never-capacity-pressured volume.

// ossAccelFlushPolicyBatchConcurrency bounds how many flush+commit-cold
// pairs the sweep issues at once. Deliberately smaller than
// ossAccelEvictionBatchConcurrency (4) — a flush is a real S3 PUT of the
// whole file's bytes (egress bandwidth + lcnode CPU for the sha256), while
// eviction's commit-cold is pure metanode metadata work with no S3 I/O at
// all. Same plain-channel-semaphore rationale as
// ossAccelEvictionBatchConcurrency's doc comment (wait-for-a-slot, not
// reject-immediately).
const ossAccelFlushPolicyBatchConcurrency = 2

type ossAccelFlushPolicyCandidate struct {
	path string
	ino  uint64
	size uint64
	sc   uint32
}

// runOssAccelFlushPolicyForVol walks vol once, flushing+committing-cold
// every never-tiered file that's been idle at least minIdleHours and is at
// least minSizeBytes. Returns how many candidates were considered
// (scanned), how many were flushed, how many were skipped by the age/size
// filter, and how many hit an error partway through (a single stuck
// candidate never aborts the whole sweep).
func (l *LcNode) runOssAccelFlushPolicyForVol(vol, prefix string, minIdleHours uint32, minSizeBytes uint64) (scanned, flushed, skipped, errors int, err error) {
	defer ossAccelObserve("flushPolicy", vol, &err)()
	mw, berr := l.buildVolMetaWrapper(vol)
	if berr != nil {
		return 0, 0, 0, 0, berr
	}
	defer mw.Close()

	volInfo, verr := l.mc.AdminAPI().GetVolumeSimpleInfo(vol)
	if verr != nil {
		return 0, 0, 0, 0, fmt.Errorf("GetVolumeSimpleInfo err: %v", verr)
	}

	// 形态收敛 pre-flight: without this, a role that forbids writing the bucket
	// still walks the whole tree and attempts a flush per candidate, each of
	// which is refused — and the loop below counts every refusal as errors++,
	// so master's LastRunResult would report "errors=N" indistinguishable from
	// real S3 failures on every single sweep, forever. Bail once, loudly, with
	// clean zero counters instead.
	//
	// Bucket-level (not per-key) on purpose: a SECONDARY with delegated
	// OwnedPrefixes must still run the sweep, since some of its candidates are
	// legitimately writable — those get the per-key check inside
	// runOssAccelFlushForVol as usual.
	roleCfg, rcerr := loadOssAccelRoleConfig(mw)
	if rcerr != nil {
		return 0, 0, 0, 0, rcerr
	}
	if !ossAccelBucketWriteAllowed(roleCfg) {
		log.LogWarnf("runOssAccelFlushPolicyForVol: vol(%v) role=%v forbids writing the bucket — skipping sweep entirely (no candidates attempted, not an error)", vol, roleCfg.Role)
		return 0, 0, 0, 0, nil
	}

	idleThreshold := time.Duration(minIdleHours) * time.Hour
	now := time.Now()

	var candidates []ossAccelFlushPolicyCandidate
	// Subtree-scoped walk when a prefix is set. This sweep is the ONLY one of
	// the six for which that is safe: its candidates have never been tiered out
	// and so carry no oss-accel.s3key, which is precisely why its filter below
	// compares the CURRENT PATH — making "start at the prefix's directory"
	// exactly equivalent to "walk everything and filter". See
	// walkOssAccelTreeUnderPathPrefix's doc comment for why the s3key-filtering
	// sweeps must not do this (for audit it would quarantine live data).
	walk := func(visit ossAccelWalkVisitor) error {
		if prefix == "" {
			return walkOssAccelTree(mw, "flushPolicy", visit)
		}
		return walkOssAccelTreeUnderPathPrefix(mw, "flushPolicy", prefix, visit)
	}
	werr := walk(func(mw *meta.MetaWrapper, parentIno uint64, path string, name string, info *proto.InodeInfo, xattrs map[string]string) error {
		// Retained even when the walk is already subtree-scoped: the prefix's
		// last component is a string prefix that may be a partial name
		// ("d1/f1" matching f1/f10/f100), which resolveOssAccelPrefixDir
		// deliberately does not resolve. This is the second gate that handles it.
		if prefix != "" && !strings.HasPrefix(normalizeOssAccelKey(path), prefix) {
			return nil
		}
		scanned++
		if xattrs[proto.XAttrKeyOSSAccelPin] == "true" {
			return nil
		}
		if xattrs[proto.XAttrKeyOSSAccelS3Key] != "" {
			return nil // already tiered out at some point — runOssAccelEvictionSweep's job, not this one's
		}
		if !proto.IsStorageClassReplica(info.StorageClass) {
			return nil // defensive — a never-tiered file should always be a replica class
		}
		if info.Size < minSizeBytes {
			skipped++
			return nil
		}
		if now.Sub(info.ModifyTime) < idleThreshold {
			skipped++
			return nil
		}
		candidates = append(candidates, ossAccelFlushPolicyCandidate{path: path, ino: info.Inode, size: info.Size, sc: info.StorageClass})
		return nil
	})
	if werr != nil {
		return scanned, 0, skipped, 0, fmt.Errorf("walkOssAccelTree err: %v", werr)
	}
	// 对齐AFM(队列深度可见性续): snapshot of what THIS sweep found, exported
	// right after the walk finishes and before any flushing starts — so it
	// reflects "how much backlog exists" even on a run where every candidate
	// later fails to flush. Not a live/persisted queue: flush-policy has no
	// queue at all, only a periodic full-tree walk (see this function's doc
	// comment), so this number is frozen between sweeps and jumps discretely
	// on the next one — a coarser signal than AFM's real queue length, but
	// the cheapest one available without turning this sweep into an index.
	exporter.NewGauge("oss_accel_flush_policy_candidates").SetWithLabels(float64(len(candidates)), map[string]string{exporter.Vol: vol})
	if len(candidates) == 0 {
		return scanned, 0, skipped, 0, nil
	}

	for start := 0; start < len(candidates); start += ossAccelFlushPolicyBatchConcurrency {
		end := start + ossAccelFlushPolicyBatchConcurrency
		if end > len(candidates) {
			end = len(candidates)
		}
		batch := candidates[start:end]

		var wg sync.WaitGroup
		results := make([]bool, len(batch))
		for i, c := range batch {
			wg.Add(1)
			go func(i int, c ossAccelFlushPolicyCandidate) {
				defer wg.Done()
				if _, _, ferr := l.runOssAccelFlushForVol(vol, c.path, c.ino, c.size, c.sc, volInfo.VolStorageClass, volInfo.AllowedStorageClass); ferr != nil {
					log.LogWarnf("runOssAccelFlushPolicyForVol: vol(%v) flush ino(%v) path(%v) err: %v", vol, c.ino, c.path, ferr)
					return // one stuck candidate shouldn't abort the whole sweep
				}
				if _, _, _, cerr := runOssAccelCommitCold(mw, vol, c.ino, c.path, 0); cerr != nil {
					log.LogWarnf("runOssAccelFlushPolicyForVol: vol(%v) commit-cold ino(%v) path(%v) err: %v", vol, c.ino, c.path, cerr)
					return
				}
				results[i] = true
			}(i, c)
		}
		wg.Wait()

		for i, ok := range results {
			if ok {
				flushed++
				log.LogInfof("runOssAccelFlushPolicyForVol: vol(%v) flushed+committed-cold ino(%v) path(%v)", vol, batch[i].ino, batch[i].path)
			} else {
				errors++
			}
		}
	}
	return scanned, flushed, skipped, errors, nil
}
