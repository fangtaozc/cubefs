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

// 反向加速续:两条不需要外部写入方配合的发现路径——定期按前缀扫桶
// (OSSAccelBucketScanRule, master-scheduled) 和显式注册 API
// (GET /ossAccelRegister, 手动触发)。两者共用同一个执行核心
// runOssAccelRegisterForVol,和 changelog sync 用的是**同一套物化逻辑**
// (materializeOssAccelChangelogEvent, lcnode/oss_accel.go) ——这里不重新
//实现物化,只是给它接上两个新的候选发现来源。
//
// changelog sync 的候选自带外部写入方上报的 checksum;这两条新路径没有
// 这个上报(外部系统根本不知道 CubeFS 存在,更不会算好 sha256 写进
// changelog),所以每个候选在首次物化前都要真下载一次算 sha256——之后
// 同一个 key 已经物化过,直接进 materializeOssAccelChangelogEvent 内部
// 的幂等/跳过分支,不会重复下载。
package lcnode

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/cubefs/cubefs/syncnode/backend"
	"github.com/cubefs/cubefs/syncnode/backend/s3"
	"github.com/cubefs/cubefs/util/log"
)

// runOssAccelRegisterForVol materializes S3 keys into cold CubeFS inodes.
// Exactly one of keys/prefix should be non-empty (validated by callers):
//   - keys non-empty: register exactly these keys (explicit single/multi-key
//     mode of GET /ossAccelRegister).
//   - keys empty, prefix non-empty: discover every non-directory key under
//     prefix via a fresh S3 List, register each one — used by both the
//     prefix mode of GET /ossAccelRegister and the scheduled
//     OSSAccelBucketScanRule sweep (opOssAccelBucketScan, lcnode/lc_op.go).
//
// Concurrency note: materializeOssAccelChangelogEvent's own Lookup_ll-then-
// Create_ll has a race window, but the loser of a concurrent race on the SAME
// key gets a clean Create_ll error (counted here as an error, never a hang or
// an orphaned inode) — the next sweep's Lookup_ll finds the winner's inode
// and takes the idempotent-refresh path instead. Verified against the exact
// class of bug the flush-policy round hit (a stuck migration-slot state under
// overlapping dispatches) — that bug was specific to commit-cold's migration
// slot handling, which this path never touches (materialization only ever
// creates BRAND NEW inodes via UpdateExtentKeyAfterMigration on a freshly
// Create_ll'd inode, never flips an existing hot inode's storage class). No
// in-flight guard needed.
func (l *LcNode) runOssAccelRegisterForVol(vol string, keys []string, prefix string) (materialized, skipped, errors int, err error) {
	defer ossAccelObserve("register", vol, &err)()

	mw, berr := l.buildVolMetaWrapper(vol)
	if berr != nil {
		return 0, 0, 0, berr
	}
	defer mw.Close()

	s3Cfg, cerr := loadOssAccelS3Config(mw, vol)
	if cerr != nil {
		return 0, 0, 0, cerr
	}
	s3Backend, nerr := s3.New(s3Cfg)
	if nerr != nil {
		return 0, 0, 0, fmt.Errorf("s3 backend init err: %v", nerr)
	}
	defer s3Backend.Close()

	ctx := context.Background()

	register := func(key string) {
		if key == "" || strings.HasPrefix(key, ossAccelTrashPrefix) || key == defaultOssAccelChangelogKey || isOssAccelReservedS3Key(key) {
			skipped++
			return
		}
		size, checksum, derr := ossAccelDownloadSizeAndChecksum(ctx, s3Backend, key)
		if derr != nil {
			log.LogWarnf("runOssAccelRegisterForVol: vol(%v) key(%v) download/checksum err: %v", vol, key, derr)
			errors++
			return
		}
		created, merr := materializeOssAccelChangelogEvent(mw, ossAccelChangelogEvent{
			Key:       key,
			Size:      size,
			Checksum:  checksum,
			EventTime: time.Now().Format(time.RFC3339),
		})
		if merr != nil {
			log.LogWarnf("runOssAccelRegisterForVol: vol(%v) key(%v) materialize err: %v", vol, key, merr)
			errors++
			return
		}
		if created {
			materialized++
			log.LogInfof("runOssAccelRegisterForVol: vol(%v) key(%v) materialized", vol, key)
			return
		}
		// Not newly created — either a POSIX-path collision with an
		// unrelated real file (materializeOssAccelChangelogEvent's own
		// refreshOssAccelChangelogOverwrite leave-alone branch, the safe
		// default) or an idempotent no-op replay of an already-materialized
		// key. Either way, nothing new happened — counted as skipped, not
		// an error.
		skipped++
	}

	if len(keys) > 0 {
		for _, k := range keys {
			register(k)
		}
		return materialized, skipped, errors, nil
	}

	if prefix == "" {
		return 0, 0, 0, fmt.Errorf("runOssAccelRegisterForVol: vol(%v) requires either keys or prefix", vol)
	}
	ch, lerr := s3Backend.List(ctx, prefix, true)
	if lerr != nil {
		return 0, 0, 0, fmt.Errorf("s3 list err: %v", lerr)
	}
	for entry := range ch {
		if entry.Err != nil {
			log.LogWarnf("runOssAccelRegisterForVol: vol(%v) prefix(%v) list entry err: %v", vol, prefix, entry.Err)
			errors++
			continue
		}
		if entry.IsDir {
			continue
		}
		register(entry.Key)
	}
	return materialized, skipped, errors, nil
}

// ossAccelDownloadSizeAndChecksum downloads key in full and returns its
// actual byte count + sha256 hex digest computed while streaming — one GET
// gives both, no separate Head needed. This is the one real cost this
// feature adds relative to changelog sync (whose events already carry a
// checksum the external writer computed): every NEWLY discovered key pays
// for a full download once; already-materialized keys never reach this path
// again (materializeOssAccelChangelogEvent's Lookup_ll dedup happens first).
func ossAccelDownloadSizeAndChecksum(ctx context.Context, s3Backend backend.Backend, key string) (size uint64, checksum string, err error) {
	body, gerr := s3Backend.Get(ctx, key, 0, -1)
	if gerr != nil {
		return 0, "", gerr
	}
	defer body.Close()
	h := sha256.New()
	n, cerr := io.Copy(h, body)
	if cerr != nil {
		return 0, "", cerr
	}
	return uint64(n), hex.EncodeToString(h.Sum(nil)), nil
}

// httpServiceOssAccelRegister handles GET /ossAccelRegister?vol=&key=|keys=|prefix=
// Exactly one of key/keys/prefix must be given: key registers a single S3
// key, keys is a comma-separated list for a handful of explicit files,
// prefix discovers and registers every key under that S3 prefix (the
// "directory" case — for large batches, prefer this over listing out keys,
// which is bounded by URL length). Manual counterpart to the scheduled
// OSSAccelBucketScanRule sweep (opOssAccelBucketScan) — both call
// runOssAccelRegisterForVol.
func (l *LcNode) httpServiceOssAccelRegister(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, fmt.Sprintf("ParseForm err: %v", err), http.StatusBadRequest)
		return
	}
	vol := r.FormValue("vol")
	if vol == "" {
		http.Error(w, "missing required form value: vol", http.StatusBadRequest)
		return
	}
	key := r.FormValue("key")
	keysParam := r.FormValue("keys")
	prefix := r.FormValue("prefix")

	provided := 0
	for _, v := range []string{key, keysParam, prefix} {
		if v != "" {
			provided++
		}
	}
	if provided != 1 {
		http.Error(w, "exactly one of key, keys, or prefix is required", http.StatusBadRequest)
		return
	}

	var keys []string
	if key != "" {
		keys = []string{key}
	} else if keysParam != "" {
		for _, k := range strings.Split(keysParam, ",") {
			k = strings.TrimSpace(k)
			if k != "" {
				keys = append(keys, k)
			}
		}
	}

	materialized, skipped, errors, err := l.runOssAccelRegisterForVol(vol, keys, prefix)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	fmt.Fprintf(w, "ok: vol=%v materialized=%v skipped=%v errors=%v\n", vol, materialized, skipped, errors)
}
