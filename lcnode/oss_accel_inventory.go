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

// 对齐AFM(即时清单查询): AFM's checkUncached/checkDirty answer "what state is
// this fileset in right now" without waiting for or triggering a full
// consistency sweep. oss-accel had no equivalent — audit/integrity/
// flushPolicy all "scan the tree, act on what they find," with no way to
// just LOOK. These two endpoints are pure reads: no xattr writes, no S3
// mutation, no role gate (there is nothing to gate — nothing here can leak
// or corrupt anything).
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
	"github.com/cubefs/cubefs/syncnode/backend/s3"
)

// errOssAccelListLimitReached is returned by a listing endpoint's walk
// visitor once it has collected enough results — walkOssAccelTree/
// walkOssAccelTreeUnderPathPrefix treat ANY non-nil visitor error as "the
// whole walk failed" (see oss_accel_walk.go's own doc comment: no existing
// convention for "stop early, this is fine"). This sentinel is that
// convention's first use: callers check errors.Is(werr, this) and treat a
// match as a normal, reportable truncation — not a failure.
var errOssAccelListLimitReached = errors.New("oss-accel list: result limit reached")

const (
	ossAccelListDefaultLimit = 500
	ossAccelListMaxLimit     = 5000
)

// ossAccelListLimit clamps the caller-requested limit into
// [1, ossAccelListMaxLimit], defaulting to ossAccelListDefaultLimit when
// absent/unparseable (parseUintForm's own fallback) or zero.
func ossAccelListLimit(r *http.Request) uint64 {
	limit := parseUintForm(r, "limit", ossAccelListDefaultLimit)
	if limit == 0 {
		limit = ossAccelListDefaultLimit
	}
	if limit > ossAccelListMaxLimit {
		limit = ossAccelListMaxLimit
	}
	return limit
}

// ossAccelColdListEntry is one row of /ossAccelListCold's result.
type ossAccelColdListEntry struct {
	path      string
	s3key     string
	size      uint64
	flushedAt string
}

// httpServiceOssAccelListCold handles GET /ossAccelListCold?vol=&prefix=&limit=
// checkUncached analog: every file whose current StorageClass is
// BlobStore — i.e. the local extent has been released and reads now go
// through the cold-read gate — as of right now, not as of the last
// scheduled sweep.
func (l *LcNode) httpServiceOssAccelListCold(w http.ResponseWriter, r *http.Request) {
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
	limit := ossAccelListLimit(r)

	mw, err := l.buildVolMetaWrapper(vol)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer mw.Close()

	var entries []ossAccelColdListEntry
	truncated := false
	// StorageClass is a current-path-safe filter (same reasoning as
	// flush-policy's — see walkOssAccelTreeUnderPathPrefix's doc comment):
	// "is this file, at its current path, cold right now" never depends on
	// a stale s3key, so prefix-scoping the walk is safe here.
	walk := func(visit ossAccelWalkVisitor) error {
		if prefix == "" {
			return walkOssAccelTree(mw, "listCold", visit)
		}
		return walkOssAccelTreeUnderPathPrefix(mw, "listCold", prefix, visit)
	}
	werr := walk(func(mw *meta.MetaWrapper, parentIno uint64, path string, name string, info *proto.InodeInfo, xattrs map[string]string) error {
		if prefix != "" && !strings.HasPrefix(normalizeOssAccelKey(path), prefix) {
			return nil
		}
		if info.StorageClass != proto.StorageClass_BlobStore {
			return nil
		}
		entries = append(entries, ossAccelColdListEntry{
			path:      path,
			s3key:     xattrs[proto.XAttrKeyOSSAccelS3Key],
			size:      info.Size,
			flushedAt: xattrs[proto.XAttrKeyOSSAccelFlushedAt],
		})
		if uint64(len(entries)) >= limit {
			return errOssAccelListLimitReached
		}
		return nil
	})
	if werr != nil {
		if errors.Is(werr, errOssAccelListLimitReached) {
			truncated = true
		} else {
			http.Error(w, fmt.Sprintf("walkOssAccelTree err: %v", werr), http.StatusInternalServerError)
			return
		}
	}

	fmt.Fprintf(w, "ok: vol=%v prefix=%v limit=%v matched=%v truncated=%v\n", vol, prefix, limit, len(entries), truncated)
	for _, e := range entries {
		fmt.Fprintf(w, "path=%v s3key=%v size=%v flushedAt=%v\n", e.path, e.s3key, e.size, e.flushedAt)
	}
}

// ossAccelFlushCandidateListEntry is one row of
// /ossAccelListFlushCandidates's result.
type ossAccelFlushCandidateListEntry struct {
	path string
	size uint64
}

// httpServiceOssAccelListFlushCandidates handles
// GET /ossAccelListFlushCandidates?vol=&prefix=&minIdleHours=&minSizeBytes=&limit=
// checkDirty analog: every file that currently satisfies flushPolicy's own
// candidate predicate (ossAccelIsFlushCandidate, oss_accel_flush_policy.go)
// but hasn't been picked up by a scheduled sweep yet.
//
// minIdleHours defaults to 0 here, unlike master's
// validateOSSAccelFlushPolicyRule (which requires >0 to keep a
// misconfigured SCHEDULED rule from flushing every never-tiered file on its
// very next run). This is a read-only inspection, not a rule that will act
// on what it finds — "show me every file that's never been tiered out,
// regardless of age" is a legitimate question with no destructive
// consequence.
func (l *LcNode) httpServiceOssAccelListFlushCandidates(w http.ResponseWriter, r *http.Request) {
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
	minIdleHours := parseUintForm(r, "minIdleHours", 0)
	minSizeBytes := parseUintForm(r, "minSizeBytes", 0)
	limit := ossAccelListLimit(r)
	idleThreshold := time.Duration(minIdleHours) * time.Hour
	now := time.Now()

	mw, err := l.buildVolMetaWrapper(vol)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer mw.Close()

	var entries []ossAccelFlushCandidateListEntry
	truncated := false
	// Safe to prefix-scope for the same reason flush-policy itself is (see
	// walkOssAccelTreeUnderPathPrefix's doc comment): the predicate never
	// depends on a stale s3key — candidates by definition have none.
	walk := func(visit ossAccelWalkVisitor) error {
		if prefix == "" {
			return walkOssAccelTree(mw, "listFlushCandidates", visit)
		}
		return walkOssAccelTreeUnderPathPrefix(mw, "listFlushCandidates", prefix, visit)
	}
	werr := walk(func(mw *meta.MetaWrapper, parentIno uint64, path string, name string, info *proto.InodeInfo, xattrs map[string]string) error {
		if prefix != "" && !strings.HasPrefix(normalizeOssAccelKey(path), prefix) {
			return nil
		}
		if ossAccelIsFlushCandidate(xattrs, info, now, minSizeBytes, idleThreshold) != ossAccelFlushCandidateMatch {
			return nil
		}
		entries = append(entries, ossAccelFlushCandidateListEntry{path: path, size: info.Size})
		if uint64(len(entries)) >= limit {
			return errOssAccelListLimitReached
		}
		return nil
	})
	if werr != nil {
		if errors.Is(werr, errOssAccelListLimitReached) {
			truncated = true
		} else {
			http.Error(w, fmt.Sprintf("walkOssAccelTree err: %v", werr), http.StatusInternalServerError)
			return
		}
	}

	fmt.Fprintf(w, "ok: vol=%v prefix=%v minIdleHours=%v minSizeBytes=%v limit=%v matched=%v truncated=%v\n",
		vol, prefix, minIdleHours, minSizeBytes, limit, len(entries), truncated)
	for _, e := range entries {
		fmt.Fprintf(w, "path=%v size=%v\n", e.path, e.size)
	}
}

// ossAccelDriftedListEntry is one row of /ossAccelListDrifted's result.
type ossAccelDriftedListEntry struct {
	path       string
	s3key      string
	verdict    ossAccelMismatchVerdict
	remoteMtime time.Time
	flushedAt  string
	checksum   string
}

// httpServiceOssAccelListDrifted handles
// GET /ossAccelListDrifted?vol=&prefix=&limit=
// checkDirty's other half: /ossAccelListFlushCandidates covers "never
// tiered out at all"; this covers "already flushed/committed cold, but the
// S3 side no longer matches what we recorded" — either legitimately
// rewritten by an external owner, or (if nothing rewrote it since our own
// flush) potentially corrupted. Reuses the exact cheap (zero-download HEAD)
// check runOssAccelIntegrityForVol's own cheap tier performs
// (ossAccelObserveS3Object + classifyOssAccelMismatch,
// oss_accel_integrity.go) — but ONLY reports what it finds. Unlike the
// scheduled integrity sweep, this endpoint never refreshes the cold
// reference and never marks ColdStateError: it is a pure inspection, the
// same stance ListCold/ListFlushCandidates take.
//
// Costs real S3 requests, unlike the other two list endpoints (which are
// pure metadata reads): one HEAD per candidate with a recorded checksum.
// limit bounds this the same way it bounds result size — a caller scoping
// this at a prefix with many cold files should expect real network time,
// not an instant in-memory answer.
func (l *LcNode) httpServiceOssAccelListDrifted(w http.ResponseWriter, r *http.Request) {
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
	limit := ossAccelListLimit(r)

	mw, err := l.buildVolMetaWrapper(vol)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer mw.Close()

	s3Cfg, err := loadOssAccelS3Config(mw, vol)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s3Backend, err := s3.New(s3Cfg)
	if err != nil {
		http.Error(w, fmt.Sprintf("s3 backend init err: %v", err), http.StatusInternalServerError)
		return
	}
	defer s3Backend.Close()

	ctx := context.Background()
	checked := 0
	var entries []ossAccelDriftedListEntry
	truncated := false
	// Same current-path-safe reasoning as ListCold — StorageClass is
	// evaluated at the file's CURRENT path, so prefix-scoping the walk
	// never misses a match the way an s3key-based filter (audit/integrity's
	// own prefix handling) could.
	walk := func(visit ossAccelWalkVisitor) error {
		if prefix == "" {
			return walkOssAccelTree(mw, "listDrifted", visit)
		}
		return walkOssAccelTreeUnderPathPrefix(mw, "listDrifted", prefix, visit)
	}
	werr := walk(func(mw *meta.MetaWrapper, parentIno uint64, path string, name string, info *proto.InodeInfo, xattrs map[string]string) error {
		if prefix != "" && !strings.HasPrefix(normalizeOssAccelKey(path), prefix) {
			return nil
		}
		if info.StorageClass != proto.StorageClass_BlobStore {
			return nil
		}
		s3key := xattrs[proto.XAttrKeyOSSAccelS3Key]
		checksum := strings.TrimPrefix(xattrs[proto.XAttrKeyOSSAccelChecksum], proto.ChecksumPrefixSHA256)
		if s3key == "" || checksum == "" {
			return nil // nothing recorded to compare the remote side against
		}
		checked++
		obs, oerr := ossAccelObserveS3Object(ctx, s3Backend, s3key)
		if oerr != nil {
			return nil // HEAD failure is inconclusive, not evidence of drift — same stance integrity takes
		}
		if !obs.Conclusive || obs.Matches(checksum) {
			return nil // matches (or nothing to conclude from) — not drifted
		}
		flushedAt := xattrs[proto.XAttrKeyOSSAccelFlushedAt]
		entries = append(entries, ossAccelDriftedListEntry{
			path: path, s3key: s3key,
			verdict:    classifyOssAccelMismatch(flushedAt, obs.Mtime),
			remoteMtime: obs.Mtime, flushedAt: flushedAt, checksum: checksum,
		})
		if uint64(len(entries)) >= limit {
			return errOssAccelListLimitReached
		}
		return nil
	})
	if werr != nil {
		if errors.Is(werr, errOssAccelListLimitReached) {
			truncated = true
		} else {
			http.Error(w, fmt.Sprintf("walkOssAccelTree err: %v", werr), http.StatusInternalServerError)
			return
		}
	}

	fmt.Fprintf(w, "ok: vol=%v prefix=%v limit=%v checked=%v drifted=%v truncated=%v\n",
		vol, prefix, limit, checked, len(entries), truncated)
	for _, e := range entries {
		fmt.Fprintf(w, "path=%v s3key=%v verdict=%v remoteMtime=%v flushedAt=%v checksum=%v\n",
			e.path, e.s3key, e.verdict, e.remoteMtime.Format(time.RFC3339), e.flushedAt, e.checksum)
	}
}
