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

// 差距分析续(对照阿里云OSS加速器"预热"功能): materializeOssAccelChangelogEvent
// (register/scan/changelog 三条发现路径共用) 只建占位 cold inode,字节内容要
// 等真实读触发 recall 才下载写入。这个文件补上"主动预热"——对已经被 CubeFS
// 认识的冷文件(StorageClass==BlobStore,不管是哪条发现路径物化的),不等真实
// 读,直接复用 recall 的核心下载写入逻辑(runOssAccelRecallForInode,
// lcnode/oss_accel.go)提前把内容拉进来。发现(知道有什么)和预热(把内容抬
// 进来)是两件正交的事——这里只做后者,前者复用 register/scan/changelog
// 已有的路径,不重新去 List S3。
package lcnode

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/syncnode/backend/s3"
	"github.com/cubefs/cubefs/util/log"
)

// ossAccelPrefetchDefaultMaxUsageRatio is the fallback capacity cap when the
// caller doesn't override it via &maxUsageRatio=. Chosen to match the kind
// of high-watermark values OSSAccelEvictionRule uses elsewhere in oss-accel
// — prefetch stopping around the same line eviction would start firing at
// avoids "prefetch fills the tier, eviction immediately undoes it" thrash.
const ossAccelPrefetchDefaultMaxUsageRatio = 0.9

// ossAccelPrefetchCandidate is one already-materialized cold file selected
// for eager recall.
type ossAccelPrefetchCandidate struct {
	path     string
	ino      uint64
	size     uint64
	checksum string
}

// ossAccelResolvePrefetchCandidate looks up a single path and classifies it
// for prefetch purposes — shared by runOssAccelPrefetchForVol's single-path
// branch and the batch prefetch task (oss_accel_prefetch_batch.go), so the
// two never drift on what counts as "already hot" vs "dangling" vs "a real
// candidate". Returns exactly one of: (candidate, false, false, nil) — a
// real BlobStore file to recall; (nil, true, false, nil) — already Replica,
// nothing to do; (nil, false, true, nil) — a prior audit marked this s3key
// unrecoverable, matches recall's own fast-fail contract; or a non-nil err
// for a genuine lookup/metadata failure.
func ossAccelResolvePrefetchCandidate(metaWrapper *meta.MetaWrapper, path string) (cand *ossAccelPrefetchCandidate, alreadyHot, dangling bool, err error) {
	ino, lerr := metaWrapper.LookupPath(path)
	if lerr != nil {
		return nil, false, false, fmt.Errorf("LookupPath(%v) err: %v", path, lerr)
	}
	info, gerr := metaWrapper.InodeGet_ll(ino)
	if gerr != nil || info == nil {
		return nil, false, false, fmt.Errorf("InodeGet_ll(%v) err: %v", ino, gerr)
	}
	if info.StorageClass != proto.StorageClass_BlobStore {
		return nil, true, false, nil
	}
	xattrs, xerr := metaWrapper.BatchGetXAttr([]uint64{ino}, []string{proto.XAttrKeyOSSAccelState, proto.XAttrKeyOSSAccelChecksum})
	var state, checksum string
	if xerr == nil && len(xattrs) > 0 {
		state = xattrs[0].XAttrs[proto.XAttrKeyOSSAccelState]
		checksum = strings.TrimPrefix(xattrs[0].XAttrs[proto.XAttrKeyOSSAccelChecksum], proto.ChecksumPrefixSHA256)
	}
	if state == proto.ColdStateError {
		return nil, false, true, nil
	}
	return &ossAccelPrefetchCandidate{path: path, ino: ino, size: info.Size, checksum: checksum}, false, false, nil
}

// runOssAccelPrefetchForVol eagerly recalls already-materialized cold files —
// path selects exactly one file, prefix walks the volume (reusing
// walkOssAccelTree, the same traversal audit/integrity use) and selects every
// file whose oss-accel.s3key has that prefix. Only StorageClass==BlobStore
// candidates are actual work; already-hot files under the same path/prefix
// are counted in alreadyHot, and files a prior audit confirmed unrecoverable
// (ColdStateError) are counted in errs without retrying them (matches
// runOssAccelRecallForInode's own fast-fail contract for that state).
//
// Capacity is checked ONCE against the full candidate set before any
// download starts — usage is monotonically non-decreasing across this single
// call, so re-checking per-candidate would be redundant work for no extra
// safety. Crossing maxUsageRatio stops the WHOLE call (stoppedForCapacity ==
// len(candidates)), reported explicitly rather than silently prefetching a
// partial, arbitrary subset.
func (l *LcNode) runOssAccelPrefetchForVol(vol, path, prefix string, vsc uint32, asc []uint32, maxUsageRatio float64) (prefetched, alreadyHot, errs, stoppedForCapacity int, err error) {
	defer ossAccelObserve("prefetch", vol, &err)()

	metaWrapper, extentClient, berr := l.buildVolClients(vol, vsc, asc)
	if berr != nil {
		return 0, 0, 0, 0, berr
	}
	defer metaWrapper.Close()
	defer extentClient.Close()

	s3Cfg, cerr := loadOssAccelS3Config(metaWrapper, vol)
	if cerr != nil {
		return 0, 0, 0, 0, cerr
	}
	s3Backend, nerr := s3.New(s3Cfg)
	if nerr != nil {
		return 0, 0, 0, 0, fmt.Errorf("s3 backend init err: %v", nerr)
	}
	defer s3Backend.Close()

	var candidates []ossAccelPrefetchCandidate

	if path != "" {
		cand, alreadyHotOne, danglingOne, rerr := ossAccelResolvePrefetchCandidate(metaWrapper, path)
		if rerr != nil {
			return 0, 0, 0, 0, rerr
		}
		if alreadyHotOne {
			return 0, 1, 0, 0, nil // already hot — nothing to prefetch
		}
		if danglingOne {
			return 0, 0, 1, 0, nil // confirmed-dangling reference — not retryable here, matches recall's own fast-fail
		}
		candidates = append(candidates, *cand)
	} else {
		werr := walkOssAccelTree(metaWrapper, "prefetch", func(mw *meta.MetaWrapper, parentIno uint64, walkPath string, name string, info *proto.InodeInfo, xattrs map[string]string) error {
			s3key := xattrs[proto.XAttrKeyOSSAccelS3Key]
			if s3key == "" || !strings.HasPrefix(s3key, prefix) {
				return nil // not oss-accel-managed under this prefix — irrelevant, don't count
			}
			if info.StorageClass != proto.StorageClass_BlobStore {
				alreadyHot++
				return nil
			}
			if xattrs[proto.XAttrKeyOSSAccelState] == proto.ColdStateError {
				errs++
				return nil
			}
			checksum := strings.TrimPrefix(xattrs[proto.XAttrKeyOSSAccelChecksum], proto.ChecksumPrefixSHA256)
			candidates = append(candidates, ossAccelPrefetchCandidate{path: walkPath, ino: info.Inode, size: info.Size, checksum: checksum})
			return nil
		})
		if werr != nil {
			return 0, alreadyHot, errs, 0, fmt.Errorf("walkOssAccelTree err: %v", werr)
		}
	}

	if len(candidates) == 0 {
		return 0, alreadyHot, errs, 0, nil
	}

	if usageRatio := ossAccelVolUsageRatio(metaWrapper); usageRatio >= maxUsageRatio {
		log.LogWarnf("runOssAccelPrefetchForVol: vol(%v) usageRatio(%.4f) >= maxUsageRatio(%.4f) — stopping before processing %v candidate(s)",
			vol, usageRatio, maxUsageRatio, len(candidates))
		return 0, alreadyHot, errs, len(candidates), nil
	}

	for _, c := range candidates {
		recallKey, dedupeWon, slotAcquired, slotErr := l.ossAccelAcquireRecallSlots(vol, c.ino)
		if !dedupeWon {
			// Someone else (a real client read, or an overlapping prefetch
			// call) is already recalling this exact inode — not a failure
			// for THIS call, the other caller's work covers it.
			log.LogInfof("runOssAccelPrefetchForVol: vol(%v) ino(%v) already being recalled elsewhere — skipping, not an error", vol, c.ino)
			continue
		}
		if !slotAcquired {
			// 差距分析续三(聚合并发无上限): total in-flight budget exhausted
			// — counted as errs (not silently skipped like the dedupe-lost
			// case above) since, unlike a dedupe loss, nobody else is doing
			// this work on this caller's behalf; a real client read hitting
			// this same file would still recall it independently, but this
			// eager-prefetch attempt itself did not complete.
			log.LogInfof("runOssAccelPrefetchForVol: vol(%v) ino(%v) %v", vol, c.ino, slotErr)
			errs++
			continue
		}
		_, _, _, rerr := l.runOssAccelRecallForInode(metaWrapper, extentClient, s3Backend, vol, c.path, c.ino, c.size, vsc, c.checksum)
		l.ossAccelReleaseRecallSlots(recallKey)
		if rerr != nil {
			log.LogWarnf("runOssAccelPrefetchForVol: vol(%v) path(%v) ino(%v) prefetch err: %v", vol, c.path, c.ino, rerr)
			errs++
			continue
		}
		prefetched++
	}

	return prefetched, alreadyHot, errs, 0, nil
}

// httpServiceOssAccelPrefetch handles
// GET /ossAccelPrefetch?vol=&path=|prefix=&sc=&vsc=&asc=[&maxUsageRatio=0.9]
// sc/vsc/asc mirror recall/flush's own storage-class params (needed to build
// the extent client that does the actual write). Exactly one of path/prefix
// is required — path recalls a single known file, prefix walks and recalls
// every already-materialized cold file under that s3key prefix.
func (l *LcNode) httpServiceOssAccelPrefetch(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, fmt.Sprintf("ParseForm err: %v", err), http.StatusBadRequest)
		return
	}
	vol := r.FormValue("vol")
	if vol == "" {
		http.Error(w, "missing required form value: vol", http.StatusBadRequest)
		return
	}
	path := r.FormValue("path")
	prefix := r.FormValue("prefix")
	if (path == "") == (prefix == "") {
		http.Error(w, "exactly one of path or prefix is required", http.StatusBadRequest)
		return
	}
	_, vsc, asc, err := parseStorageClassForm(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	maxUsageRatio := ossAccelPrefetchDefaultMaxUsageRatio
	if raw := r.FormValue("maxUsageRatio"); raw != "" {
		parsed, perr := strconv.ParseFloat(raw, 64)
		if perr != nil {
			http.Error(w, fmt.Sprintf("ParseFloat maxUsageRatio err: %v", perr), http.StatusBadRequest)
			return
		}
		maxUsageRatio = parsed
	}

	prefetched, alreadyHot, errs, stoppedForCapacity, err := l.runOssAccelPrefetchForVol(vol, path, prefix, vsc, asc, maxUsageRatio)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	fmt.Fprintf(w, "ok: vol=%v prefetched=%v alreadyHot=%v errors=%v stoppedForCapacity=%v\n", vol, prefetched, alreadyHot, errs, stoppedForCapacity)
}
