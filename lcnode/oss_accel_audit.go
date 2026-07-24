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

// Bidirectional consistency audit between CubeFS's oss-accel namespace and
// the external S3 bucket it tiers to. Two independent directions, both
// covered in a single tree-walk + single bucket-list pass so neither costs
// more than doing them separately:
//
//   - CubeFS→S3 ("dangling reference"): an inode's oss-accel.s3key xattr
//     points at a key that no longer exists in the bucket (external
//     deletion, or a prior bug). Unambiguous — always a real problem — so
//     it's reported and marked immediately, no grace period needed.
//   - S3→CubeFS ("orphan"): a bucket key has no inode currently referencing
//     it. This is ambiguous on its own — it could be freshly-written
//     external data not yet consumed via changelog, or a placeholder that
//     M2's TTL sweep correctly reclaimed (S3 object intentionally left
//     alone), or a genuine leak (a file this cluster itself flushed out was
//     later deleted via plain POSIX unlink, which has no oss-accel hook to
//     clean up the S3 side). A grace period (default 24h) gives the benign
//     cases time to resolve before a key is treated as a leak; even then,
//     the action is quarantine (rename into .trash/), never delete — matches
//     this project's standing "never destroy, only rename/mark" discipline
//     (M1 delayed-release grace period, M4 relocate's fail-closed conflict
//     check).
package lcnode

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/syncnode/backend"
	"github.com/cubefs/cubefs/syncnode/backend/s3"
	"github.com/cubefs/cubefs/util/log"
)

// ossAccelTrashPrefix is where AUDIT-2 quarantines suspected orphans.
// Renaming (not deleting) means a false positive is always recoverable by
// renaming back — the same reasoning as M1's delayed-release grace period.
const ossAccelTrashPrefix = ".trash/"

// ossAccelDefaultOrphanGraceHours is how old an unreferenced S3 key must be
// before AUDIT-2 treats it as a candidate leak rather than "freshly written,
// not consumed yet". Overridable per-call via the orphanGraceHours query
// param; not a per-vol persisted setting — this is a manually-triggered
// audit (mirrors M1 flush / M2 changelog-sync / M4 relocate all being
// manual-first), not a scheduled job with its own config surface yet.
const ossAccelDefaultOrphanGraceHours = 24

// ossAccelReservedS3Prefixes are S3 key prefixes known to belong to OTHER
// systems sharing this bucket, never to oss-accel — a fast pre-filter so
// Direction B's orphan scan doesn't bother Head-confirming keys that are
// obviously not ours. This list is NOT the safety mechanism anymore (see
// ossAccelOwnerMetadataKey below) — it only saves a Head call for the
// common, already-known-foreign cases; anything that slips past this list
// still gets the ownership-marker check before it can ever be quarantined.
//
// Real-machine incident (2026-07-22): an unscoped audit run (prefix="")
// quarantined 74 Terraform/OpenTofu remote-state files
// (envs/<env>/<component>/terraform.tfstate) that share this bucket with
// oss-accel's test data — restored by hand. The underlying gap this
// exposed ("no notion of 'this key belongs to someone else'") is what the
// marker+confirm scheme below actually closes; this prefix list remains
// only as a cheap optimization, not the thing preventing a repeat.
var ossAccelReservedS3Prefixes = []string{
	"envs/", // this repo's own Terraform/OpenTofu S3 remote-state layout (see cubefs-deploy/root.hcl)
}

func isOssAccelReservedS3Key(key string) bool {
	for _, p := range ossAccelReservedS3Prefixes {
		if strings.HasPrefix(key, p) {
			return true
		}
	}
	return false
}

// ossAccelOwnerMetadataKey/Value is the ownership marker every oss-accel
// flush stamps on its S3 objects (see httpServiceOssAccelFlush's Put call).
// Direction B's orphan scan uses this — not the reserved-prefix list above
// — as the actual safety mechanism before quarantining a key: no marker
// means "not written by this oss-accel deployment," full stop, regardless
// of what prefix it happens to live under. This is what makes the 2026-07-22
// incident (see ossAccelReservedS3Prefixes) structurally impossible to
// repeat — a shared bucket can gain arbitrary new foreign prefixes in the
// future and none of them need to be enumerated here.
//
// Known limitation: objects flushed before this marker existed have no way
// to acquire it retroactively short of being re-flushed — they'll be
// treated as foreign (skipped, never quarantined) rather than losing their
// orphan-detection coverage in the unsafe direction. A one-off backfill
// job could re-tag them if that coverage gap matters in practice; not done
// here.
const (
	ossAccelOwnerMetadataKey   = "oss-accel-owner"
	ossAccelOwnerMetadataValue = "cubefs"
)

// isOssAccelOwnedS3Key confirms key actually carries the ownership marker
// before it's allowed to be treated as an orphan candidate. Treats any
// Head/Stat failure (including ErrKeyNotFound — a benign list/head race on
// a key deleted between the two calls) as "not confirmed ours" — the safe
// direction is to skip, never to quarantine on missing information.
func isOssAccelOwnedS3Key(ctx context.Context, s3Backend backend.Backend, key string) bool {
	stater, ok := s3Backend.(backend.Stater)
	if !ok {
		// Backend doesn't support metadata passthrough at all — can't
		// confirm ownership, so nothing is ever quarantined. Correct for
		// today (oss-accel's cold backend is always the s3 kind, which
		// does implement Stater), and safe even if that ever changes.
		return false
	}
	st, err := stater.Stat(ctx, key)
	if err != nil {
		log.LogWarnf("isOssAccelOwnedS3Key: Stat key(%v) err(%v) — treating as not confirmed ours", key, err)
		return false
	}
	return st.RawMetadata[ossAccelOwnerMetadataKey] == ossAccelOwnerMetadataValue
}

// httpServiceOssAccelAudit handles GET /ossAccelAudit?vol=&prefix=&orphanGraceHours=
func (l *LcNode) httpServiceOssAccelAudit(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, fmt.Sprintf("ParseForm err: %v", err), http.StatusBadRequest)
		return
	}
	vol := r.FormValue("vol")
	if vol == "" {
		http.Error(w, "missing required form value: vol", http.StatusBadRequest)
		return
	}
	prefix := r.FormValue("prefix")
	graceHours := parseUintForm(r, "orphanGraceHours", ossAccelDefaultOrphanGraceHours)

	result, err := l.runOssAccelAuditForVol(vol, prefix, graceHours)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	fmt.Fprintf(w, "ok: vol=%v prefix=%v dangling=%v orphansConsidered=%v quarantined=%v relocated=%v driftConflicts=%v\ndanglingKeys=%v\nquarantinedKeys=%v\nrelocatedKeys=%v\ndriftConflictKeys=%v\n",
		vol, prefix, len(result.DanglingKeys), len(result.OrphanCandidateKeys), len(result.QuarantinedKeys), len(result.RelocatedKeys), len(result.DriftConflictKeys),
		result.DanglingKeys, result.QuarantinedKeys, result.RelocatedKeys, result.DriftConflictKeys)
}

// runOssAccelAuditForVol builds this volume's meta/S3 clients and runs one
// audit pass — the shared setup both httpServiceOssAccelAudit (manual
// trigger) and opOssAccelAudit (系统层面收尾: master-scheduled AdminTask,
// lcnode/lc_op.go) call, so the construction logic isn't duplicated
// between the two entry points.
func (l *LcNode) runOssAccelAuditForVol(vol, prefix string, graceHours uint64) (ossAccelAuditResult, error) {
	metaWrapper, err := l.buildVolMetaWrapper(vol)
	if err != nil {
		return ossAccelAuditResult{}, err
	}
	defer metaWrapper.Close()

	s3Cfg, err := loadOssAccelS3Config(metaWrapper, vol)
	if err != nil {
		return ossAccelAuditResult{}, err
	}
	s3Backend, err := s3.New(s3Cfg)
	if err != nil {
		return ossAccelAuditResult{}, fmt.Errorf("s3 backend init err: %v", err)
	}
	defer s3Backend.Close()

	return runOssAccelAudit(metaWrapper, s3Backend, prefix, time.Duration(graceHours)*time.Hour)
}

// ossAccelAuditResult carries not just counts but the actual affected keys —
// an operator (or a caller cleaning up after a mis-scoped run, as real-
// machine testing needed to do once) needs to know WHICH files, not just
// how many.
type ossAccelAuditResult struct {
	DanglingKeys        []string
	OrphanCandidateKeys []string
	QuarantinedKeys     []string
	RelocatedKeys       []string
	DriftConflictKeys   []string
}

// runOssAccelAudit does the single tree-walk + single bucket-list pass
// described in the package doc comment above. It also folds in a third
// direction (Direction C) during the same tree-walk: a cold file's current
// path no longer matching its oss-accel.s3key xattr means a POSIX rename
// happened without going through the manual /ossAccelRelocate endpoint —
// see runOssAccelRelocate's doc comment for why auto-fixing this is safe
// (fail-closed conflict check, never overwrites).
func runOssAccelAudit(mw *meta.MetaWrapper, s3Backend backend.Backend, prefix string, orphanGrace time.Duration) (result ossAccelAuditResult, err error) {
	ctx := context.Background()
	knownKeys := make(map[string]struct{})
	// inode/name pairs whose s3key needs an existence check — collected
	// during the walk, resolved against s3Keys after the bucket listing
	// completes (so a single List call serves both directions).
	type danglingCandidate struct {
		ino   uint64
		s3key string
	}
	var toCheck []danglingCandidate

	werr := walkOssAccelTree(mw, func(mw *meta.MetaWrapper, parentIno uint64, path string, name string, info *proto.InodeInfo, xattrs map[string]string) error {
		s3key := xattrs[proto.XAttrKeyOSSAccelS3Key]
		if s3key == "" || !strings.HasPrefix(s3key, prefix) {
			// The prefix scopes BOTH sides of the diff — a file whose
			// s3key falls outside prefix is invisible to the S3 List call
			// below too, so it must also be excluded here. Checking only
			// the List side while walking the WHOLE tree regardless of
			// prefix would make every cold file outside prefix look
			// "missing" (its real key is never enumerated) — real-machine
			// testing caught exactly this as a mass false-positive on the
			// first version of this function.
			return nil
		}
		if info.StorageClass != proto.StorageClass_BlobStore {
			// Hot file, possibly carrying a stale s3key xattr from a prior
			// cold period (now recalled) — still counts for Direction B's
			// "is this key referenced by anything" check, but Direction C
			// (rename-drift) only matters for currently-cold files: nothing
			// reads via a hot file's xattr, so relocating it serves no
			// purpose.
			knownKeys[s3key] = struct{}{}
			return nil
		}

		// Direction C: rename-drift auto-fix. Only within prefix on BOTH
		// ends — a file renamed from inside prefix to outside it is out of
		// this call's jurisdiction; leave it for whichever audit call
		// covers the destination, don't let a side effect escape the
		// caller-declared scope.
		if expectedKey := normalizeOssAccelKey(path); expectedKey != s3key && strings.HasPrefix(expectedKey, prefix) {
			switch rerr := runOssAccelRelocate(ctx, mw, s3Backend, info.Inode, s3key, expectedKey); {
			case rerr == nil:
				log.LogInfof("runOssAccelAudit: auto-relocated drifted key ino(%v) %v -> %v (path=%v)", info.Inode, s3key, expectedKey, path)
				result.RelocatedKeys = append(result.RelocatedKeys, expectedKey)
				// The old key no longer exists (Rename moved it) — Direction
				// A's dangling-check below must use the NEW key for this
				// candidate, or it would find the old key "missing" and
				// wrongly flag a file this audit call just fixed.
				s3key = expectedKey
			case errors.Is(rerr, errOssAccelRelocateConflict):
				result.DriftConflictKeys = append(result.DriftConflictKeys, s3key)
				log.LogWarnf("runOssAccelAudit: drift target already occupied, not relocating ino(%v) %v -> %v: %v", info.Inode, s3key, expectedKey, rerr)
			default:
				// e.g. transient S3/network error, or oldKey itself already
				// missing (genuinely dangling, not just drifted) — leave
				// s3key unchanged and let Direction A's own existence check
				// below make the call.
				log.LogWarnf("runOssAccelAudit: drift relocate attempt failed ino(%v) %v -> %v: %v", info.Inode, s3key, expectedKey, rerr)
			}
		}

		knownKeys[s3key] = struct{}{}
		toCheck = append(toCheck, danglingCandidate{ino: info.Inode, s3key: s3key})
		return nil
	})
	if werr != nil {
		return result, fmt.Errorf("tree walk err: %v", werr)
	}

	ch, lerr := s3Backend.List(ctx, prefix, true)
	if lerr != nil {
		return result, fmt.Errorf("s3 list err: %v", lerr)
	}
	s3Keys := make(map[string]time.Time)
	for entry := range ch {
		if entry.Err != nil {
			return result, fmt.Errorf("s3 list entry err: %v", entry.Err)
		}
		if entry.IsDir || strings.HasPrefix(entry.Key, ossAccelTrashPrefix) || entry.Key == defaultOssAccelChangelogKey || isOssAccelReservedS3Key(entry.Key) {
			continue
		}
		s3Keys[entry.Key] = entry.Mtime
	}

	// Direction A: dangling references — unambiguous, mark immediately.
	for _, c := range toCheck {
		if _, ok := s3Keys[c.s3key]; ok {
			continue
		}
		result.DanglingKeys = append(result.DanglingKeys, c.s3key)
		log.LogErrorf("runOssAccelAudit: dangling reference ino(%v) s3key(%v) — object missing in S3, marking ColdStateError", c.ino, c.s3key)
		if serr := mw.BatchSetXAttr_ll(c.ino, map[string]string{proto.XAttrKeyOSSAccelState: proto.ColdStateError}); serr != nil {
			log.LogWarnf("runOssAccelAudit: ino(%v) failed to mark ColdStateError: %v", c.ino, serr)
		}
	}

	// Direction B: orphan candidates — ambiguous, grace period + quarantine only.
	now := time.Now()
	for key, mtime := range s3Keys {
		if _, ok := knownKeys[key]; ok {
			continue
		}
		if now.Sub(mtime) < orphanGrace {
			continue // too fresh — may just not be consumed via changelog yet
		}
		if !isOssAccelOwnedS3Key(ctx, s3Backend, key) {
			// Confirmed not ours (or unconfirmable) — never quarantine.
			// This, not the reserved-prefix list, is what makes the
			// 2026-07-22 shared-bucket incident structurally impossible
			// to repeat: any foreign key, under any prefix, is safe by
			// default.
			continue
		}
		result.OrphanCandidateKeys = append(result.OrphanCandidateKeys, key)
		trashKey := ossAccelTrashPrefix + key
		if rerr := s3Backend.Rename(ctx, key, trashKey); rerr != nil {
			log.LogWarnf("runOssAccelAudit: failed to quarantine orphan key(%v) -> %v: %v", key, trashKey, rerr)
			continue
		}
		result.QuarantinedKeys = append(result.QuarantinedKeys, key)
		log.LogWarnf("runOssAccelAudit: quarantined orphan key(%v) -> %v (age %v, no CubeFS reference)", key, trashKey, now.Sub(mtime))
	}

	return result, nil
}
