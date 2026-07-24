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
	"sort"
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/util/log"
)

// ossAccelEvictionBatchConcurrency bounds how many commit-cold calls the
// sweep issues at once (阶段 T — "回收扫描不抢占带宽", user-confirmed
// scope: no interactive/normal/bulk tiers, just cap the sweep's own
// concurrency so it never competes hard with a client's concurrent,
// unthrottled /ossAccelRecall). A plain semaphore (buffered channel) rather
// than util/concurrent.KeyConcurrentLimit — that utility's Acquire is
// reject-immediately (returns ErrLimit over the cap), which would need an
// awkward retry/spin loop to turn into a wait-for-a-slot worker pool; a
// channel is the idiomatic Go primitive for exactly this "at most N
// in-flight" shape and needs no extra dependency, new or otherwise.
const ossAccelEvictionBatchConcurrency = 4

// M3 容量治理 — coldest-first eviction sweep (阶段 R). Dispatched via
// OpLcNodeOssAccelEvict (opOssAccelEvict, lcnode/lc_op.go), master-scheduled
// by OSSAccelEvictionRuleManager when a volume's usage ratio crosses its
// HighWatermarkRatio (master/oss_accel_eviction_rule_manager.go).
//
// Candidate = oss-accel.s3key present (this file was tiered out and
// recalled via oss-accel at some point) && StorageClass is a replica class
// (currently resident in the hot tier — a BlobStore-class file is already
// cold, nothing to evict) && not pinned. Ranked by
// oss-accel.lastRecallTime ascending (oldest/least-recently-recalled
// first) — see proto.XAttrKeyOSSAccelLastRecallTime's doc comment for why
// this is a purpose-built signal rather than metanode's native AccessTime.
// A candidate with no lastRecallTime xattr (recalled by lcnode code
// predating this field, or otherwise missing it) sorts first — treated as
// "oldest of all," the conservative choice for a signal that's supposed to
// protect recently-used data.

type ossAccelEvictCandidate struct {
	parentIno  uint64
	path       string
	name       string
	ino        uint64
	lastRecall time.Time // zero value sorts first, see doc comment above
}

// runOssAccelEvictionSweep walks vol once, evicting (commit-cold) the
// coldest oss-accel-managed resident files until usage drops to
// lowWatermarkRatio or candidates run out. Returns how many candidates were
// considered, how many were actually evicted, and the resulting usage
// ratio (all needed by master to decide whether another round is needed —
// see proto.OSSAccelEvictionTaskResponse).
func (l *LcNode) runOssAccelEvictionSweep(vol string, lowWatermarkRatio float64) (considered, evicted int, usageRatioAfter float64, err error) {
	defer ossAccelObserve("evict", vol, &err)()
	mw, berr := l.buildVolMetaWrapper(vol)
	if berr != nil {
		return 0, 0, 0, berr
	}
	defer mw.Close()

	usageRatioAfter = ossAccelVolUsageRatio(mw)

	var candidates []ossAccelEvictCandidate
	werr := walkOssAccelTree(mw, func(mw *meta.MetaWrapper, parentIno uint64, path string, name string, info *proto.InodeInfo, xattrs map[string]string) error {
		if xattrs[proto.XAttrKeyOSSAccelPin] == "true" {
			return nil
		}
		if xattrs[proto.XAttrKeyOSSAccelS3Key] == "" {
			return nil // never tiered out via oss-accel — not ours to touch
		}
		if !proto.IsStorageClassReplica(info.StorageClass) {
			return nil // already cold (or some other class) — nothing to evict
		}
		var lastRecall time.Time
		if ts := xattrs[proto.XAttrKeyOSSAccelLastRecallTime]; ts != "" {
			if parsed, perr := time.Parse(time.RFC3339, ts); perr == nil {
				lastRecall = parsed
			}
		}
		candidates = append(candidates, ossAccelEvictCandidate{parentIno: parentIno, path: path, name: name, ino: info.Inode, lastRecall: lastRecall})
		return nil
	})
	if werr != nil {
		return 0, 0, usageRatioAfter, fmt.Errorf("walkOssAccelTree err: %v", werr)
	}
	considered = len(candidates)
	if considered == 0 {
		return 0, 0, usageRatioAfter, nil
	}

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].lastRecall.Before(candidates[j].lastRecall)
	})

	// Batches of up to ossAccelEvictionBatchConcurrency run concurrently
	// (bounded — see the constant's doc comment); usage ratio is
	// re-checked BETWEEN batches, not after every single eviction, so a
	// batch can mildly overshoot lowWatermarkRatio but concurrency stays
	// capped and the stop condition still gets checked often enough to
	// matter (batch size 4, not "all candidates at once").
	for start := 0; start < len(candidates); start += ossAccelEvictionBatchConcurrency {
		if usageRatioAfter <= lowWatermarkRatio {
			break
		}
		end := start + ossAccelEvictionBatchConcurrency
		if end > len(candidates) {
			end = len(candidates)
		}
		batch := candidates[start:end]

		var wg sync.WaitGroup
		results := make([]bool, len(batch))
		for i, c := range batch {
			wg.Add(1)
			go func(i int, c ossAccelEvictCandidate) {
				defer wg.Done()
				if _, _, _, cerr := runOssAccelCommitCold(mw, vol, c.ino, c.path, 0); cerr != nil {
					log.LogWarnf("runOssAccelEvictionSweep: vol(%v) commit-cold ino(%v) name(%v) err: %v", vol, c.ino, c.name, cerr)
					return // one stuck candidate (e.g. lease still valid) shouldn't abort the whole sweep
				}
				results[i] = true
			}(i, c)
		}
		wg.Wait()

		for i, ok := range results {
			if ok {
				evicted++
				log.LogInfof("runOssAccelEvictionSweep: vol(%v) evicted ino(%v) name(%v) lastRecall(%v)",
					vol, batch[i].ino, batch[i].name, batch[i].lastRecall)
			}
		}
		usageRatioAfter = ossAccelVolUsageRatio(mw)
	}
	return considered, evicted, usageRatioAfter, nil
}

// ossAccelVolUsageRatio reads the volume's current usage ratio via the meta
// client's own Statfs (lcnode has no direct access to master's Vol struct —
// this is the client-side equivalent of the exact same ratio
// OSSAccelEvictionRuleManager checks server-side to decide whether to fire).
func ossAccelVolUsageRatio(mw *meta.MetaWrapper) float64 {
	total, used, _ := mw.Statfs()
	if total == 0 {
		return 0
	}
	return float64(used) / float64(total)
}
