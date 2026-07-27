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

// Shared full-tree walker for oss-accel's periodic background sweeps — M2
// 收尾's placeholder TTL cleanup (oss_accel_sweep.go) and M3's coldest-first
// eviction sweep both need "walk every regular file under the volume root,
// evaluate its oss-accel xattrs + StorageClass, act on matches" and neither
// has an index to consult (no existing "list inodes matching xattr X" query
// anywhere in this codebase) — so both do a plain recursive directory walk,
// modeled on lc_scanner.go's ReadDirLimit_ll pagination (handleDirLimitDepthFirst),
// but as a simple synchronous function (no scanner/task/channel machinery —
// this is lcnode-internal periodic maintenance, not a master-dispatched
// RuleTask scan).
//
// Still a full tree walk on every invocation — the asymptotic fix is an index,
// not a smarter walk, and that remains unattempted. But the per-file constant
// factor has been cut: metadata for a whole directory page is fetched in two
// batched RPCs instead of two RPCs PER FILE (see walkOssAccelDir). Six sweeps
// now share this walker, so that constant mattered.
package lcnode

import (
	"os"
	"syscall"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/util/log"
)

// ossAccelWalkPageSize mirrors lc_scanner.go's defaultReadDirLimit.
const ossAccelWalkPageSize = 1000

// ossAccelWalkVisitor is called once per regular file encountered during the
// walk, with its already-fetched xattrs (oss-accel-prefixed keys only — see
// ossAccelWalkXAttrKeys) and its full current path (e.g. "/ckpt/model.bin",
// assembled from the walk's own directory recursion — cheap, since the walk
// already knows every ancestor name it descended through). Returning a
// non-nil error aborts the walk entirely (a single bad candidate shouldn't
// be allowed to silently swallow the rest of the sweep, so callers should
// log-and-continue internally rather than return an error for a single
// skippable candidate).
type ossAccelWalkVisitor func(mw *meta.MetaWrapper, parentIno uint64, path string, name string, info *proto.InodeInfo, xattrs map[string]string) error

// ossAccelWalkXAttrKeys is the fixed set of xattrs fetched for every regular
// file visited — covers every key any current or planned sweep consults
// (state, pin, last-recall-time, checksum, last-integrity-check-time),
// fetched once per file rather than once per sweep-specific concern.
var ossAccelWalkXAttrKeys = []string{
	proto.XAttrKeyOSSAccelState,
	proto.XAttrKeyOSSAccelPin,
	proto.XAttrKeyOSSAccelLastRecallTime,
	proto.XAttrKeyOSSAccelS3Key,
	proto.XAttrKeyOSSAccelChecksum,
	proto.XAttrKeyOSSAccelLastIntegrityCheckTime,
	proto.XAttrKeyOSSAccelFlushedAt,
}

// walkOssAccelTree recursively walks the volume from its root, calling visit
// for every regular file. Pin-checking is NOT done here — every sweep
// callback is expected to check xattrs[proto.XAttrKeyOSSAccelPin] itself
// (kept explicit at the call site rather than silently skipped here, so a
// sweep's own logging/counters see pinned files as "considered, excluded"
// rather than the walker hiding them entirely).
// sweep names the calling sweep, for the one summary log line emitted per walk.
// Six sweeps share this walker and several can be scheduled on the same lcnode,
// so an unlabeled line would be unattributable — and the whole point of the
// line is to make walk cost measurable per sweep.
func walkOssAccelTree(mw *meta.MetaWrapper, sweep string, visit ossAccelWalkVisitor) error {
	stats := &ossAccelWalkStats{start: time.Now()}
	err := walkOssAccelDir(mw, proto.RootIno, "", stats, visit)
	stats.log(sweep, err)
	return err
}

// ossAccelWalkStats is the per-walk cost summary. Deliberately counting pages
// rather than RPCs: pages are what the batching changed, so "files per page" is
// the number that shows whether batching is actually being exercised (a tree of
// tiny directories gets little benefit no matter how large the volume is —
// that distinction is exactly what decides whether parallel descent is worth
// adding next).
type ossAccelWalkStats struct {
	start time.Time
	dirs  int
	files int
	pages int
}

func (s *ossAccelWalkStats) log(sweep string, err error) {
	elapsed := time.Since(s.start)
	perPage := 0.0
	if s.pages > 0 {
		perPage = float64(s.files) / float64(s.pages)
	}
	if err != nil {
		log.LogWarnf("ossAccelWalk[%v]: aborted after dirs(%v) files(%v) pages(%v) in %v: %v",
			sweep, s.dirs, s.files, s.pages, elapsed, err)
		return
	}
	log.LogInfof("ossAccelWalk[%v]: dirs(%v) files(%v) pages(%v) filesPerPage(%.1f) elapsed(%v)",
		sweep, s.dirs, s.files, s.pages, perPage, elapsed)
}

func walkOssAccelDir(mw *meta.MetaWrapper, dirIno uint64, dirPath string, stats *ossAccelWalkStats, visit ossAccelWalkVisitor) error {
	stats.dirs++
	marker := ""
	for {
		stats.pages++
		children, err := mw.ReadDirLimit_ll(dirIno, marker, ossAccelWalkPageSize)
		if err != nil && err != syscall.ENOENT {
			return err
		}
		if err == syscall.ENOENT {
			return nil
		}
		if marker != "" && len(children) >= 1 && children[0].Name == marker {
			if len(children) <= 1 {
				return nil
			}
			children = children[1:]
		}

		// Two passes over the page. Directories recurse immediately, exactly as
		// before, so depth-first order is unchanged. Regular files are collected
		// first and their metadata fetched in TWO batched RPCs for the whole page
		// instead of two per file — mw.BatchInodeGet and mw.BatchGetXAttr both
		// group by meta partition internally (sdk/meta/api.go), so a 1000-file
		// page costs ~2×(number of MPs) round trips rather than 2000.
		//
		// visit is still called once per file in the page's original order, with
		// the same arguments as before, so none of the six sweeps needed to
		// change — and the skip semantics are preserved exactly: a file whose
		// inode raced away is dropped, and a file with no xattrs at all gets an
		// empty (non-nil) map.
		files := make([]proto.Dentry, 0, len(children))
		for _, child := range children {
			if os.FileMode(child.Type).IsDir() {
				if werr := walkOssAccelDir(mw, child.Inode, dirPath+"/"+child.Name, stats, visit); werr != nil {
					return werr
				}
				continue
			}
			files = append(files, child)
		}
		stats.files += len(files)
		for _, entry := range fetchOssAccelWalkPage(mw, files) {
			if verr := visit(mw, dirIno, dirPath+"/"+entry.dentry.Name, entry.dentry.Name, entry.info, entry.attrs); verr != nil {
				return verr
			}
		}

		childrenNr := len(children)
		if (marker == "" && childrenNr < ossAccelWalkPageSize) || (marker != "" && childrenNr+1 < ossAccelWalkPageSize) {
			return nil
		}
		marker = children[childrenNr-1].Name
	}
}

// ossAccelWalkPageEntry pairs a dentry with the metadata fetched for it.
type ossAccelWalkPageEntry struct {
	dentry proto.Dentry
	info   *proto.InodeInfo
	attrs  map[string]string
}

// fetchOssAccelWalkPage does the two batched metadata RPCs for one directory
// page and hands back the results in the page's original order.
func fetchOssAccelWalkPage(mw *meta.MetaWrapper, files []proto.Dentry) []ossAccelWalkPageEntry {
	if len(files) == 0 {
		return nil
	}
	inodes := make([]uint64, 0, len(files))
	for _, f := range files {
		inodes = append(inodes, f.Inode)
	}
	// BatchInodeGet has no error return: it drops whatever it could not fetch,
	// which assembleOssAccelWalkPage then treats as "raced away".
	infos := mw.BatchInodeGet(inodes)
	// A BatchGetXAttr error is not fatal — it degrades every file in this page
	// to "no xattrs", the same thing the old per-file code did when its
	// single-inode call failed. Logged rather than swallowed silently, since at
	// page granularity one failure now costs up to a whole page of attrs.
	xattrs, xerr := mw.BatchGetXAttr(inodes, ossAccelWalkXAttrKeys)
	if xerr != nil {
		log.LogWarnf("fetchOssAccelWalkPage: BatchGetXAttr failed for %v inode(s), treating the page as having no oss-accel xattrs: %v",
			len(inodes), xerr)
		xattrs = nil
	}
	return assembleOssAccelWalkPage(files, infos, xattrs)
}

// assembleOssAccelWalkPage joins a directory page's dentries with the batched
// inode and xattr results.
//
// Split out as a pure function because this join is where the batching's real
// hazards live, and they are invisible in a small test volume:
//   - BOTH batch APIs return results that are UNORDERED and MAY BE SHORTER than
//     the input. BatchGetXAttr only emits an entry for inodes that have an
//     extendTree record at all (metanode/partition_op_extend.go), so every file
//     with no xattrs is simply absent; BatchInodeGet fans out one goroutine per
//     meta partition and collects over a channel. Matching by slice index instead
//     of by inode number would silently attach one file's metadata to another —
//     which for these sweeps means acting on the wrong file.
//   - Output order must stay the page's dentry order, because that is what the
//     pre-batching walker produced and the sweeps' logs/results were read
//     against.
//
// A file whose inode is missing is dropped (it was deleted between the readdir
// and the fetch). A file whose xattrs are missing gets an empty, non-nil map.
func assembleOssAccelWalkPage(files []proto.Dentry, infos []*proto.InodeInfo, xattrs []*proto.XAttrInfo) []ossAccelWalkPageEntry {
	infoByIno := make(map[uint64]*proto.InodeInfo, len(infos))
	for _, info := range infos {
		if info != nil {
			infoByIno[info.Inode] = info
		}
	}
	attrsByIno := make(map[uint64]map[string]string, len(xattrs))
	for _, x := range xattrs {
		if x != nil {
			attrsByIno[x.Inode] = x.XAttrs
		}
	}

	out := make([]ossAccelWalkPageEntry, 0, len(files))
	for _, f := range files {
		info, ok := infoByIno[f.Inode]
		if !ok {
			continue // raced away between ReadDirLimit_ll and the batch fetch
		}
		attrs := attrsByIno[f.Inode]
		if attrs == nil {
			// Preserve the pre-batching contract: visitors index this map
			// directly, so it must never be nil even when the file has no
			// xattrs at all (the common case for an unmanaged file).
			attrs = map[string]string{}
		}
		out = append(out, ossAccelWalkPageEntry{dentry: f, info: info, attrs: attrs})
	}
	return out
}
