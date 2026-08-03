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

// 对齐AFM: metanode has no delete hook of any kind — plain POSIX unlink
// (fsmUnlinkInode/internalDeleteInode) is unaware oss-accel exists, so a
// direct `rm` on a tiered-out file leaves its S3 object orphaned until
// audit's set-difference sweep finds it (up to orphanGraceHours later,
// then only quarantined to .trash/, never deleted outright — see
// oss_accel_audit.go / oss_accel_trash_purge.go). This endpoint does NOT
// change that: it is an EXPLICIT, opt-in second step, not a hook. A caller
// that already knows it is about to delete an oss-accel-managed file (e.g.
// a future oss-accel-aware rm wrapper) calls this FIRST to reclaim the S3
// object synchronously, then performs its own normal POSIX unlink. Nothing
// here fires automatically on unlink — that would require touching
// metanode, which is out of scope (see docs/plan for why).
package lcnode

import (
	"context"
	"fmt"
	"net/http"
	"strconv"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/syncnode/backend/s3"
)

// httpServiceOssAccelDelete handles GET /ossAccelDelete?vol=&path=&ino=
// Synchronously deletes the S3 object backing this inode, if any. The
// caller is responsible for the POSIX unlink itself — this only reclaims
// the cold-tier copy, on the same path/ino the caller is about to remove.
func (l *LcNode) httpServiceOssAccelDelete(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, fmt.Sprintf("ParseForm err: %v", err), http.StatusBadRequest)
		return
	}
	vol := r.FormValue("vol")
	path := r.FormValue("path")
	if vol == "" || path == "" {
		http.Error(w, "missing required form value: vol and path", http.StatusBadRequest)
		return
	}
	ino, err := strconv.ParseUint(r.FormValue("ino"), 10, 64)
	if err != nil {
		http.Error(w, fmt.Sprintf("ParseUint ino err: %v", err), http.StatusBadRequest)
		return
	}

	deleted, s3key, derr := l.runOssAccelDeleteForVol(vol, ino)
	if derr != nil {
		httpErrorOssAccel(w, derr)
		return
	}
	if !deleted {
		fmt.Fprintf(w, "ok: vol=%v ino=%v path=%v nothing to delete (no oss-accel.s3key xattr — file was never flushed)\n", vol, ino, path)
		return
	}
	fmt.Fprintf(w, "ok: vol=%v ino=%v path=%v s3key=%v deleted from cold backend\n", vol, ino, path, s3key)
}

// runOssAccelDeleteForVol reads ino's oss-accel.s3key xattr (must be read
// BEFORE the caller's own unlink — once the inode is gone, so is the
// xattr) and, if present, deletes the corresponding S3 object. Absence of
// the xattr is not an error: most files are never tiered out, and a caller
// shouldn't have to pre-check "is this file oss-accel-managed" before
// calling this endpoint.
func (l *LcNode) runOssAccelDeleteForVol(vol string, ino uint64) (deleted bool, s3key string, err error) {
	defer ossAccelObserve("delete", vol, &err)()
	metaWrapper, err := l.buildVolMetaWrapper(vol)
	if err != nil {
		return false, "", err
	}
	defer metaWrapper.Close()

	xattrs, xerr := metaWrapper.BatchGetXAttr([]uint64{ino}, []string{proto.XAttrKeyOSSAccelS3Key})
	if xerr != nil {
		return false, "", fmt.Errorf("BatchGetXAttr err: %v", xerr)
	}
	if len(xattrs) == 0 {
		return false, "", nil // never flushed — nothing on the cold side to reclaim
	}
	s3key = xattrs[0].XAttrs[proto.XAttrKeyOSSAccelS3Key]
	if s3key == "" {
		return false, "", nil
	}

	// This permanently destroys S3 data (unlike flush's additive upload), so
	// it must pass the same write gate every other destructive oss-accel path
	// (trashPurge, commit-cold's local-extent release) checks — not a single
	// exception to the role model.
	roleCfg, rcerr := loadOssAccelRoleConfig(metaWrapper)
	if rcerr != nil {
		return false, "", rcerr
	}
	if !ossAccelKeyWriteAllowed(roleCfg, s3key) {
		return false, "", ossAccelWriteForbiddenErrf(roleCfg, vol, "delete", s3key)
	}

	s3Cfg, err := loadOssAccelS3Config(metaWrapper, vol)
	if err != nil {
		return false, "", err
	}
	s3Backend, err := s3.New(s3Cfg)
	if err != nil {
		return false, "", fmt.Errorf("s3 backend init err: %v", err)
	}
	defer s3Backend.Close()

	if derr := s3Backend.Delete(context.Background(), s3key); derr != nil {
		return false, "", fmt.Errorf("s3 delete err: %v", derr)
	}
	return true, s3key, nil
}
