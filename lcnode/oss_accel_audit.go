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

	metaWrapper, err := l.buildVolMetaWrapper(vol)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer metaWrapper.Close()

	s3Cfg, err := loadOssAccelS3Config(metaWrapper, vol)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	s3Backend, err := s3.New(s3Cfg)
	if err != nil {
		http.Error(w, fmt.Sprintf("s3 backend init err: %v", err), http.StatusInternalServerError)
		return
	}
	defer s3Backend.Close()

	result, err := runOssAccelAudit(metaWrapper, s3Backend, prefix, time.Duration(graceHours)*time.Hour)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	fmt.Fprintf(w, "ok: vol=%v prefix=%v dangling=%v orphansConsidered=%v quarantined=%v\ndanglingKeys=%v\nquarantinedKeys=%v\n",
		vol, prefix, len(result.DanglingKeys), len(result.OrphanCandidateKeys), len(result.QuarantinedKeys),
		result.DanglingKeys, result.QuarantinedKeys)
}

// ossAccelAuditResult carries not just counts but the actual affected keys —
// an operator (or a caller cleaning up after a mis-scoped run, as real-
// machine testing needed to do once) needs to know WHICH files, not just
// how many.
type ossAccelAuditResult struct {
	DanglingKeys        []string
	OrphanCandidateKeys []string
	QuarantinedKeys     []string
}

// runOssAccelAudit does the single tree-walk + single bucket-list pass
// described in the package doc comment above.
func runOssAccelAudit(mw *meta.MetaWrapper, s3Backend backend.Backend, prefix string, orphanGrace time.Duration) (result ossAccelAuditResult, err error) {
	knownKeys := make(map[string]struct{})
	// inode/name pairs whose s3key needs an existence check — collected
	// during the walk, resolved against s3Keys after the bucket listing
	// completes (so a single List call serves both directions).
	type danglingCandidate struct {
		ino   uint64
		s3key string
	}
	var toCheck []danglingCandidate

	werr := walkOssAccelTree(mw, func(mw *meta.MetaWrapper, parentIno uint64, name string, info *proto.InodeInfo, xattrs map[string]string) error {
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
		knownKeys[s3key] = struct{}{}
		if info.StorageClass == proto.StorageClass_BlobStore {
			toCheck = append(toCheck, danglingCandidate{ino: info.Inode, s3key: s3key})
		}
		return nil
	})
	if werr != nil {
		return result, fmt.Errorf("tree walk err: %v", werr)
	}

	ctx := context.Background()
	ch, lerr := s3Backend.List(ctx, prefix, true)
	if lerr != nil {
		return result, fmt.Errorf("s3 list err: %v", lerr)
	}
	s3Keys := make(map[string]time.Time)
	for entry := range ch {
		if entry.Err != nil {
			return result, fmt.Errorf("s3 list entry err: %v", entry.Err)
		}
		if entry.IsDir || strings.HasPrefix(entry.Key, ossAccelTrashPrefix) || entry.Key == defaultOssAccelChangelogKey {
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
