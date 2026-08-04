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
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/syncnode/backend"
	"github.com/cubefs/cubefs/syncnode/backend/s3"
	"github.com/cubefs/cubefs/util/log"
)

// 系统层面收尾续(补1+3) — cold-tier integrity verification. Dispatched via
// OpLcNodeOssAccelIntegrity (opOssAccelIntegrity, lcnode/lc_op.go),
// master-scheduled by OSSAccelIntegrityRuleManager on a plain elapsed-time
// poll (master/oss_accel_integrity_rule_manager.go).
//
// Candidate = XAttrKeyOSSAccelS3Key present && StorageClass is NOT a replica
// class (i.e. actually resident in the cold tier right now — a file that's
// been flushed but not yet committed cold has no local copy released, so
// there's nothing "at risk" to verify yet).
//
// Two independently-costed tiers, both run every sweep:
//   - cheap: EVERY candidate gets a zero-download HeadObject, comparing the
//     S3-side "syncnode-sha256" user-metadata (stamped at flush time, see
//     backend.SHA256MetadataKey) against the xattr-recorded checksum. Catches
//     only "S3-side metadata was changed out from under us" — NOT bit rot,
//     since the bytes are never read.
//   - full: up to FullSampleCount candidates (the ones with the OLDEST
//     XAttrKeyOSSAccelLastIntegrityCheckTime — zero value, i.e. never
//     checked, sorts first) get an actual download + re-hash, the same
//     comparison recall already does (runOssAccelRecallWrite). This is the
//     only tier that can catch genuine silent bit rot, at real S3 egress +
//     lcnode CPU cost — sampled and rotated rather than run on every
//     candidate every time.
//
// A mismatch on EITHER tier only marks the file proto.ColdStateError and
// logs the specifics — no attempt at automatic repair. Once a file has been
// committed cold, its S3 copy is the ONLY copy; there is nothing left
// locally to re-flush from, so "detect and mark" is the complete action
// here, not a partial one. ColdStateError is already consumed by the
// existing recall path (lcnode/oss_accel.go, AUDIT-1) — a subsequent read
// attempt fails fast with a distinguishable error instead of hanging or
// silently returning corrupt bytes.
//
// 形态收敛 EXCEPTION: on a volume whose backing bucket is externally owned
// (role=readonly), a mismatch is DETECTED and REPORTED but NOT marked. Two
// reasons: against a bucket someone else writes, a changed checksum most
// likely means the owner legitimately updated the object rather than that it
// rotted; and ColdStateError has no clearing path for a committed-cold file
// (nothing writes ColdStateClean without a hot copy to re-flush), so a false
// positive there is a permanent read-block, not a recoverable warning. The
// MismatchesUnmarked counter reports exactly how many took this path.

// ossAccelIntegrityBatchConcurrency bounds how many full-tier downloads run
// at once — real S3 egress bandwidth, same wait-for-a-slot semantics as
// ossAccelFlushPolicyBatchConcurrency's doc comment. The cheap tier (plain
// HeadObject, no body bytes) runs sequentially — it's cheap enough on both
// sides that a concurrency cap buys little and only adds complexity.
const ossAccelIntegrityBatchConcurrency = 2

type ossAccelIntegrityCandidate struct {
	path      string
	ino       uint64
	s3key     string
	checksum  string // bare hex, sha256: prefix already stripped
	lastCheck time.Time
	// flushedAt is the raw RFC3339 XAttrKeyOSSAccelFlushedAt value ("" when
	// the file predates that xattr) — the discriminator input for
	// classifyOssAccelMismatch. Kept raw rather than pre-parsed so the
	// "unparsable is as uninformative as missing" rule lives in one place.
	flushedAt string
}

// ossAccelIntegrityResult is what one sweep found. Grouped into a struct
// rather than grown to a fifth naked int return — MismatchesUnmarked was the
// point at which positional returns stopped being readable at the call site.
type ossAccelIntegrityResult struct {
	CheapChecked int
	FullChecked  int
	// Mismatches counts every mismatch DETECTED, marked or not — unchanged
	// meaning from before 形态收敛.
	Mismatches int
	// MismatchesUnmarked ⊆ Mismatches: detected but deliberately not marked
	// ColdStateError because the bucket is externally owned (role=readonly).
	MismatchesUnmarked int
	// MismatchesRefreshed ⊆ Mismatches: the remote object's mtime proved it was
	// rewritten AFTER our last flush, so this was a legitimate external update
	// and the cold reference was refreshed to follow it instead of being
	// flagged. Without this counter, "we followed an external update" and "we
	// did nothing" look identical from outside — the same reason the *Refused
	// counters exist.
	MismatchesRefreshed int
}

// runOssAccelIntegrityForVol builds this volume's meta/S3 clients and runs
// one integrity sweep — mirrors runOssAccelAuditForVol's construction
// pattern.
func (l *LcNode) runOssAccelIntegrityForVol(vol, prefix string, fullSampleCount uint32) (result ossAccelIntegrityResult, err error) {
	defer ossAccelObserve("integrity", vol, &err)()
	mw, berr := l.buildVolMetaWrapper(vol)
	if berr != nil {
		return result, berr
	}
	defer mw.Close()

	s3Cfg, err := loadOssAccelS3Config(mw, vol)
	if err != nil {
		return result, err
	}
	s3Backend, err := s3.New(s3Cfg)
	if err != nil {
		return result, fmt.Errorf("s3 backend init err: %v", err)
	}
	defer s3Backend.Close()

	// 形态收敛: decides whether a detected mismatch gets marked ColdStateError.
	// Uses the externally-owned predicate, NOT the write predicate — a secondary
	// shares a collectively-owned bucket and must keep marking; only readonly
	// suppresses. Loaded once per sweep.
	roleCfg, rcerr := loadOssAccelRoleConfig(mw)
	if rcerr != nil {
		return result, rcerr
	}
	markMismatch := !ossAccelBucketExternallyOwned(roleCfg)

	var candidates []ossAccelIntegrityCandidate
	werr := walkOssAccelTree(mw, "integrity", func(mw *meta.MetaWrapper, parentIno uint64, path string, name string, info *proto.InodeInfo, xattrs map[string]string) error {
		s3key := xattrs[proto.XAttrKeyOSSAccelS3Key]
		if s3key == "" {
			return nil // never tiered out — nothing cold to verify
		}
		if proto.IsStorageClassReplica(info.StorageClass) {
			return nil // flushed but not (yet) committed cold — no released local copy at risk
		}
		if prefix != "" && !strings.HasPrefix(s3key, prefix) {
			return nil
		}
		checksum := strings.TrimPrefix(xattrs[proto.XAttrKeyOSSAccelChecksum], proto.ChecksumPrefixSHA256)
		if checksum == "" {
			return nil // no recorded checksum to compare against — nothing to verify
		}
		var lastCheck time.Time
		if ts := xattrs[proto.XAttrKeyOSSAccelLastIntegrityCheckTime]; ts != "" {
			if parsed, perr := time.Parse(time.RFC3339, ts); perr == nil {
				lastCheck = parsed
			}
		}
		candidates = append(candidates, ossAccelIntegrityCandidate{
			path: path, ino: info.Inode, s3key: s3key, checksum: checksum, lastCheck: lastCheck,
			flushedAt: xattrs[proto.XAttrKeyOSSAccelFlushedAt],
		})
		return nil
	})
	if werr != nil {
		return result, fmt.Errorf("walkOssAccelTree err: %v", werr)
	}
	if len(candidates) == 0 {
		return result, nil
	}

	ctx := context.Background()

	// Cheap tier: every candidate, sequential, zero-download.
	for _, c := range candidates {
		result.CheapChecked++
		obs, cerr := ossAccelObserveS3Object(ctx, s3Backend, c.s3key)
		if cerr != nil {
			log.LogWarnf("runOssAccelIntegrityForVol: vol(%v) cheap-check ino(%v) s3key(%v) err: %v", vol, c.ino, c.s3key, cerr)
			continue // Stat failure is inconclusive, not evidence of corruption — same "safe by default" as isOssAccelOwnedS3Key
		}
		if !obs.Conclusive || obs.Matches(c.checksum) {
			continue
		}
		result.Mismatches++

		// 差距分析续(漂移自动刷新): a mismatch alone doesn't say WHY. Use the
		// remote mtime vs our own flushedAt to tell "the owner rewrote it"
		// apart from "it rotted", instead of assuming one of them.
		switch classifyOssAccelMismatch(c.flushedAt, obs.Mtime) {
		case ossAccelMismatchExternalUpdate:
			if rerr := refreshOssAccelDriftedCold(mw, c.ino, c.s3key, obs); rerr != nil {
				log.LogWarnf("runOssAccelIntegrityForVol: vol(%v) ino(%v) s3key(%v) external update detected (remote mtime %v > flushedAt %v) but refresh failed: %v — leaving the stale cold reference in place rather than half-updating it",
					vol, c.ino, c.s3key, obs.Mtime, c.flushedAt, rerr)
				continue
			}
			result.MismatchesRefreshed++
			log.LogInfof("runOssAccelIntegrityForVol: vol(%v) ino(%v) path(%v) s3key(%v) followed external update (remote mtime %v > flushedAt %v): checksum/size refreshed, no download needed",
				vol, c.ino, c.path, c.s3key, obs.Mtime, c.flushedAt)
		case ossAccelMismatchUninterpretable:
			// No usable flushedAt — most likely a cold file written before that
			// xattr existed. Fall back to the pre-existing behavior rather than
			// guessing; the file gets a flushedAt on its next flush.
			if !markMismatch {
				result.MismatchesUnmarked++
			}
			reportOssAccelIntegrityMismatch(mw, c.ino, c.path, c.s3key,
				fmt.Sprintf("cheap(metadata); no usable oss-accel.flushedAt (%q) so external-update vs corruption is undecidable", c.flushedAt), markMismatch)
		default: // ossAccelMismatchSuspectCorruption
			if !markMismatch {
				result.MismatchesUnmarked++
			}
			reportOssAccelIntegrityMismatch(mw, c.ino, c.path, c.s3key,
				fmt.Sprintf("cheap(metadata); remote mtime %v is NOT newer than flushedAt %v, so nothing rewrote it since we did", obs.Mtime, c.flushedAt), markMismatch)
		}
	}

	// Full tier: up to fullSampleCount candidates, oldest-checked first.
	if fullSampleCount > 0 {
		sort.Slice(candidates, func(i, j int) bool {
			return candidates[i].lastCheck.Before(candidates[j].lastCheck)
		})
		sampleEnd := int(fullSampleCount)
		if sampleEnd > len(candidates) {
			sampleEnd = len(candidates)
		}
		sample := candidates[:sampleEnd]

		for start := 0; start < len(sample); start += ossAccelIntegrityBatchConcurrency {
			end := start + ossAccelIntegrityBatchConcurrency
			if end > len(sample) {
				end = len(sample)
			}
			batch := sample[start:end]

			var wg sync.WaitGroup
			batchMismatches := make([]bool, len(batch))
			for i, c := range batch {
				wg.Add(1)
				go func(i int, c ossAccelIntegrityCandidate) {
					defer wg.Done()
					match, actual, ferr := ossAccelFullChecksumMatches(ctx, s3Backend, c.s3key, c.checksum)
					if ferr != nil {
						log.LogWarnf("runOssAccelIntegrityForVol: vol(%v) full-check ino(%v) s3key(%v) err: %v", vol, c.ino, c.s3key, ferr)
						return // download/hash failure is inconclusive, not evidence of corruption
					}
					if !match {
						batchMismatches[i] = true
						reportOssAccelIntegrityMismatch(mw, c.ino, c.path, c.s3key, fmt.Sprintf("full(expected=%v actual=%v)", c.checksum, actual), markMismatch)
					}
				}(i, c)
			}
			wg.Wait()

			for i := range batch {
				result.FullChecked++
				if batchMismatches[i] {
					result.Mismatches++
					if !markMismatch {
						result.MismatchesUnmarked++
					}
				}
				// Stamp regardless of outcome — rotation must advance even on
				// a mismatch, or a permanently-corrupt file would monopolize
				// every future sample.
				if serr := mw.BatchSetXAttr_ll(batch[i].ino, map[string]string{
					proto.XAttrKeyOSSAccelLastIntegrityCheckTime: time.Now().Format(time.RFC3339),
				}); serr != nil {
					log.LogWarnf("runOssAccelIntegrityForVol: vol(%v) ino(%v) failed to stamp lastIntegrityCheckTime: %v", vol, batch[i].ino, serr)
				}
			}
		}
	}

	return result, nil
}

// ossAccelS3Observation is what a single cheap (HeadObject) probe learned
// about the remote object. 差距分析续(漂移自动刷新) replaced the old
// bool-returning ossAccelCheapChecksumMatches with this: collapsing the probe
// to "matches / doesn't match" at the bottom threw away the two things needed
// to INTERPRET a mismatch — the object's mtime (is it newer than our flush?)
// and its current checksum/size (what would we refresh TO?). Both were already
// being fetched and discarded.
type ossAccelS3Observation struct {
	// Checksum is the remote object's own recorded sha256 (bare hex, from the
	// syncnode-sha256 user metadata). Empty when the object carries none.
	Checksum string
	Size     int64
	// Mtime is the remote object's last-modified time, from the same clock
	// that stamped XAttrKeyOSSAccelFlushedAt at flush time.
	Mtime time.Time
	// Conclusive is false when the probe cannot support ANY conclusion:
	// the backend doesn't expose metadata, or the object records no checksum.
	// Callers must treat !Conclusive as "no evidence either way" — never as a
	// match and never as a mismatch.
	Conclusive bool
}

// Matches reports whether the remote object's checksum equals wantChecksum.
// Only meaningful when Conclusive.
func (o ossAccelS3Observation) Matches(wantChecksum string) bool {
	return o.Checksum == wantChecksum
}

// ossAccelObserveS3Object HEADs key (no body download) and returns what the
// remote side says about it. A missing checksum metadata value (e.g. the
// best-effort multipart stamp failed at flush time — see
// httpServiceOssAccelFlush's doc comment) yields Conclusive=false rather than
// a false mismatch: absence of evidence is not evidence of corruption.
func ossAccelObserveS3Object(ctx context.Context, s3Backend backend.Backend, key string) (ossAccelS3Observation, error) {
	stater, ok := s3Backend.(backend.Stater)
	if !ok {
		// Backend doesn't support metadata passthrough — nothing to check,
		// matches isOssAccelOwnedS3Key's fail-open direction.
		return ossAccelS3Observation{}, nil
	}
	st, err := stater.Stat(ctx, key)
	if err != nil {
		return ossAccelS3Observation{}, err
	}
	got := st.RawMetadata[backend.SHA256MetadataKey]
	if got == "" {
		return ossAccelS3Observation{Size: st.Size, Mtime: st.Mtime}, nil
	}
	return ossAccelS3Observation{Checksum: got, Size: st.Size, Mtime: st.Mtime, Conclusive: true}, nil
}

// ossAccelMismatchVerdict is how the sweep decided to interpret one mismatch.
type ossAccelMismatchVerdict int

const (
	// ossAccelMismatchUninterpretable: no flushedAt recorded (a cold file that
	// predates that xattr), so "newer than our write" cannot be evaluated.
	// Falls back to the pre-existing mark/don't-mark behavior.
	ossAccelMismatchUninterpretable ossAccelMismatchVerdict = iota
	// ossAccelMismatchExternalUpdate: the remote object was modified AFTER our
	// last flush — somebody else legitimately rewrote it. Refresh to follow.
	ossAccelMismatchExternalUpdate
	// ossAccelMismatchSuspectCorruption: nothing wrote the object since we did,
	// yet the content no longer matches. Do NOT silently follow it.
	ossAccelMismatchSuspectCorruption
)

// String renders the verdict for text responses — the read-only
// /ossAccelListDrifted endpoint (oss_accel_inventory.go) uses this to report
// what it found without duplicating the mark/refresh decision this sweep
// makes on the same verdict.
func (v ossAccelMismatchVerdict) String() string {
	switch v {
	case ossAccelMismatchExternalUpdate:
		return "externalUpdate"
	case ossAccelMismatchSuspectCorruption:
		return "suspectCorruption"
	default:
		return "uninterpretable"
	}
}

// classifyOssAccelMismatch applies the flushedAt-vs-Mtime discriminator. Split
// out as a pure function so the truth table is unit-testable without S3 or a
// metanode (see oss_accel_integrity_test.go).
func classifyOssAccelMismatch(flushedAtRaw string, remoteMtime time.Time) ossAccelMismatchVerdict {
	if flushedAtRaw == "" {
		return ossAccelMismatchUninterpretable
	}
	flushedAt, perr := time.Parse(time.RFC3339, flushedAtRaw)
	if perr != nil {
		// An unparsable value is no better evidence than a missing one.
		return ossAccelMismatchUninterpretable
	}
	if remoteMtime.IsZero() {
		return ossAccelMismatchUninterpretable
	}
	if remoteMtime.After(flushedAt) {
		return ossAccelMismatchExternalUpdate
	}
	return ossAccelMismatchSuspectCorruption
}

// refreshOssAccelDriftedCold follows a confirmed external update: point the
// inode's recorded checksum/size at whatever the remote object now holds.
//
// ZERO DOWNLOAD — the new size and checksum both come from the cheap probe's
// Stat. That is the whole reason this path is worth having: the bucket-scan
// discovery path already refreshes externally-overwritten objects, but pays a
// full GET + re-hash per key per sweep to do it.
//
// Reuses refreshOssAccelChangelogOverwrite (lcnode/oss_accel.go), which is
// already the single implementation of "an external writer replaced the object
// at this key, update our cold reference" — the changelog-sync and
// bucket-scan paths both go through it. Two things it deliberately does NOT
// do, which this wrapper must therefore handle itself:
//   - it leaves oss-accel.state alone, so a file a PREVIOUS sweep marked
//     ColdStateError would stay unreadable even after a successful refresh;
//     reset to clean explicitly.
//   - it doesn't touch flushedAt, so the same update would be re-judged on
//     every subsequent sweep; advance it to the observed mtime.
func refreshOssAccelDriftedCold(mw *meta.MetaWrapper, ino uint64, s3key string, obs ossAccelS3Observation) error {
	if _, err := refreshOssAccelChangelogOverwrite(mw, ino, ossAccelChangelogEvent{
		Key:      s3key,
		Size:     uint64(obs.Size),
		Checksum: obs.Checksum,
	}); err != nil {
		return err
	}
	return mw.BatchSetXAttr_ll(ino, map[string]string{
		proto.XAttrKeyOSSAccelState:     proto.ColdStateClean,
		proto.XAttrKeyOSSAccelFlushedAt: obs.Mtime.UTC().Format(time.RFC3339),
	})
}

// ossAccelFullChecksumMatches downloads key in full and re-hashes it — the
// identical sha256-over-a-streamed-body computation runOssAccelRecallWrite
// already does (lcnode/oss_accel.go), just without writing the bytes
// anywhere (this is a read-only verification, not a recall).
func ossAccelFullChecksumMatches(ctx context.Context, s3Backend backend.Backend, key, wantChecksum string) (match bool, actual string, err error) {
	body, gerr := s3Backend.Get(ctx, key, 0, -1)
	if gerr != nil {
		return false, "", gerr
	}
	defer body.Close()
	h := sha256.New()
	if _, cerr := io.Copy(h, body); cerr != nil {
		return false, "", cerr
	}
	actual = hex.EncodeToString(h.Sum(nil))
	return actual == wantChecksum, actual, nil
}

// reportOssAccelIntegrityMismatch logs a detected mismatch and, when mark is
// true, marks ino ColdStateError — see the package-level doc comment above for
// why marking is the complete action rather than a partial one, and for why
// an externally-owned bucket (role=readonly) suppresses it.
//
// Named "report" rather than "mark" precisely because marking is now
// conditional: a caller reading `mark...(...)` would reasonably assume the
// mark always happens.
func reportOssAccelIntegrityMismatch(mw *meta.MetaWrapper, ino uint64, path, s3key, detail string, mark bool) {
	if !mark {
		log.LogWarnf("reportOssAccelIntegrityMismatch: checksum mismatch ino(%v) path(%v) s3key(%v) detail(%v) — NOT marking ColdStateError: bucket is externally owned (role=readonly), so a mismatch is more likely a legitimate update by the bucket's owner than corruption, and ColdStateError is unclearable for a committed-cold file",
			ino, path, s3key, detail)
		return
	}
	if serr := mw.BatchSetXAttr_ll(ino, map[string]string{proto.XAttrKeyOSSAccelState: proto.ColdStateError}); serr != nil {
		log.LogWarnf("reportOssAccelIntegrityMismatch: ino(%v) path(%v) s3key(%v) failed to mark ColdStateError: %v", ino, path, s3key, serr)
	}
	log.LogErrorf("reportOssAccelIntegrityMismatch: checksum mismatch ino(%v) path(%v) s3key(%v) detail(%v) — marked ColdStateError", ino, path, s3key, detail)
}
