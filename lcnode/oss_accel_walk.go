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
// Not optimized for huge namespaces: this is a full tree walk on every
// invocation, the same scaling characteristic lc_scanner.go itself has. If
// the managed namespace grows large enough for this to matter, the fix is an
// index, not a smarter walk — not attempted here.
package lcnode

import (
	"os"
	"syscall"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/meta"
)

// ossAccelWalkPageSize mirrors lc_scanner.go's defaultReadDirLimit.
const ossAccelWalkPageSize = 1000

// ossAccelWalkVisitor is called once per regular file encountered during the
// walk, with its already-fetched xattrs (oss-accel-prefixed keys only — see
// ossAccelWalkXAttrKeys). Returning a non-nil error aborts the walk entirely
// (a single bad candidate shouldn't be allowed to silently swallow the rest
// of the sweep, so callers should log-and-continue internally rather than
// return an error for a single skippable candidate).
type ossAccelWalkVisitor func(mw *meta.MetaWrapper, parentIno uint64, name string, info *proto.InodeInfo, xattrs map[string]string) error

// ossAccelWalkXAttrKeys is the fixed set of xattrs fetched for every regular
// file visited — covers every key any current or planned sweep consults
// (state, pin, last-recall-time), fetched once per file rather than once per
// sweep-specific concern.
var ossAccelWalkXAttrKeys = []string{
	proto.XAttrKeyOSSAccelState,
	proto.XAttrKeyOSSAccelPin,
	proto.XAttrKeyOSSAccelLastRecallTime,
	proto.XAttrKeyOSSAccelS3Key,
}

// walkOssAccelTree recursively walks the volume from its root, calling visit
// for every regular file. Pin-checking is NOT done here — every sweep
// callback is expected to check xattrs[proto.XAttrKeyOSSAccelPin] itself
// (kept explicit at the call site rather than silently skipped here, so a
// sweep's own logging/counters see pinned files as "considered, excluded"
// rather than the walker hiding them entirely).
func walkOssAccelTree(mw *meta.MetaWrapper, visit ossAccelWalkVisitor) error {
	return walkOssAccelDir(mw, proto.RootIno, visit)
}

func walkOssAccelDir(mw *meta.MetaWrapper, dirIno uint64, visit ossAccelWalkVisitor) error {
	marker := ""
	for {
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

		for _, child := range children {
			if os.FileMode(child.Type).IsDir() {
				if werr := walkOssAccelDir(mw, child.Inode, visit); werr != nil {
					return werr
				}
				continue
			}
			info, gerr := mw.InodeGet_ll(child.Inode)
			if gerr != nil || info == nil {
				continue // raced away (deleted between ReadDirLimit_ll and InodeGet_ll) — skip, not fatal
			}
			xattrs, xerr := mw.BatchGetXAttr([]uint64{child.Inode}, ossAccelWalkXAttrKeys)
			attrs := map[string]string{}
			if xerr == nil && len(xattrs) > 0 {
				attrs = xattrs[0].XAttrs
			}
			if verr := visit(mw, dirIno, child.Name, info, attrs); verr != nil {
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
