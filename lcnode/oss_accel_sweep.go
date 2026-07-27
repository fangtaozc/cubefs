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
	"os"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/util/log"
)

// runOssAccelPlaceholderSweep reclaims M2-materialized placeholder inodes
// that have sat unread past their TTL (M2 收尾 阶段 N). Piggybacks on the
// SAME per-vol dispatch cadence as changelog sync (OSSAccelChangelogRule's
// IntervalSeconds via OSSAccelChangelogRuleManager) rather than adding a
// second, independent lcnode-owned ticker — a TTL sweep and a changelog
// sync are both "periodic per-vol maintenance driven by the same rule," so
// piggybacking is the smaller, more coherent change (no new lcnode
// self-ticker, no new master "list configured vols" endpoint to discover
// what to sweep).
//
// Candidate = oss-accel.state==materialized (never successfully recalled —
// a recall flips StorageClass away from BlobStore without touching this
// xattr, see proto.ColdStateMaterialized) && StorageClass==BlobStore &&
// CreateTime+ttl < now && not pinned.
func (l *LcNode) runOssAccelPlaceholderSweep(vol string, ttlSeconds uint32) (swept int, err error) {
	if ttlSeconds == 0 {
		return 0, nil
	}
	mw, berr := l.buildVolMetaWrapper(vol)
	if berr != nil {
		return 0, berr
	}
	defer mw.Close()

	deadline := time.Now().Add(-time.Duration(ttlSeconds) * time.Second)
	werr := walkOssAccelTree(mw, "placeholderSweep", func(mw *meta.MetaWrapper, parentIno uint64, path string, name string, info *proto.InodeInfo, xattrs map[string]string) error {
		if os.FileMode(info.Mode).IsDir() {
			return nil
		}
		if xattrs[proto.XAttrKeyOSSAccelPin] == "true" {
			return nil
		}
		if xattrs[proto.XAttrKeyOSSAccelState] != proto.ColdStateMaterialized {
			return nil
		}
		if info.StorageClass != proto.StorageClass_BlobStore {
			return nil // already recalled at least once — not a sweep candidate
		}
		if info.CreateTime.After(deadline) {
			return nil // not old enough yet
		}
		if _, derr := mw.Delete_ll(parentIno, name, false, path); derr != nil {
			log.LogErrorf("runOssAccelPlaceholderSweep: vol(%v) Delete_ll(%v,%v) err: %v", vol, parentIno, name, derr)
			return nil // don't abort the whole sweep over one candidate
		}
		if eerr := mw.Evict(info.Inode, path); eerr != nil {
			log.LogWarnf("runOssAccelPlaceholderSweep: vol(%v) Evict(%v) err: %v", vol, info.Inode, eerr)
		}
		swept++
		log.LogInfof("runOssAccelPlaceholderSweep: vol(%v) reclaimed unread placeholder ino(%v) name(%v) createTime(%v)",
			vol, info.Inode, name, info.CreateTime)
		return nil
	})
	if werr != nil {
		return swept, fmt.Errorf("walkOssAccelTree err: %v", werr)
	}
	return swept, nil
}
