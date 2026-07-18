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

// OSS accelerator client-side cold read gate. When a file's StorageClass is
// BlobStore and carries the oss-accel xattrs (its bytes live in an external S3
// bucket, not native ObjExtentKeys — see proto/oss_accel.go), a plain read
// through the native blobstore reader finds nothing. This gate synchronously
// asks a mover (lcnode) to recall the file back into the replica tier before
// the read proceeds.
//
// M1 policy (deliberately minimal): if the mover reports the file isn't safe to
// recall yet (its delayed-release migration slot from a prior tier-out hasn't
// been cleaned up — HTTP 425), the gate returns a plain retryable errno rather
// than spinning or polling internally. The caller (an application read()) sees
// EAGAIN and decides whether to retry. This never returns a zero-filled read —
// avoiding the class of "cold file silently reads as holes" bug this fork's
// review work has repeatedly flagged as unacceptable.

package fs

import (
	"fmt"
	"io"
	"net/http"
	"syscall"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

// ossAccelHTTPClient is shared across gate calls; recall itself may stream a
// large object, so the timeout here bounds only the mover's initial commitment,
// not the full body transfer — the mover response only arrives once its own
// S3-GET-and-write loop (or its early 425 rejection) has completed.
var ossAccelHTTPClient = &http.Client{Timeout: 5 * time.Minute}

// ossAccelColdReadGate is called by File.Read before falling back to the native
// blobstore reader on a StorageClass=BlobStore file. Returns:
//   - nil, false: not an oss-accel file (no oss-accel xattrs) — caller proceeds
//     to the native blobstore reader unchanged.
//   - nil, true: recall succeeded — caller's inode cache entry has been
//     invalidated; caller should re-fetch info and retry via the replica path.
//   - non-nil error: oss-accel is enabled and this is a cold file, but it isn't
//     safely readable right now. Always a distinguishable errno, never a
//     reason to return zero-filled data.
func (s *Super) ossAccelColdReadGate(ino uint64) (err error, recalled bool) {
	if len(s.ossAccelMoverAddrs) == 0 {
		return nil, false
	}

	xattrs, gerr := s.mw.BatchGetXAttr([]uint64{ino}, []string{
		proto.XAttrKeyOSSAccelS3Key, proto.XAttrKeyOSSAccelChecksum, proto.XAttrKeyOSSAccelSize,
	})
	if gerr != nil || len(xattrs) == 0 {
		log.LogWarnf("ossAccelColdReadGate: BatchGetXAttr ino(%v) err(%v)", ino, gerr)
		return nil, false
	}
	s3key := xattrs[0].XAttrs[proto.XAttrKeyOSSAccelS3Key]
	if s3key == "" {
		// BlobStore but not an oss-accel file (e.g. native blobstore volume) —
		// fall back to the native reader unchanged.
		return nil, false
	}
	checksum := xattrs[0].XAttrs[proto.XAttrKeyOSSAccelChecksum]
	sizeStr := xattrs[0].XAttrs[proto.XAttrKeyOSSAccelSize]

	url := fmt.Sprintf("http://%s/ossAccelRecall?vol=%s&ino=%d&size=%s&sc=%d&vsc=%d&asc=%d&path=%s&checksum=%s",
		s.ossAccelMoverAddrs[0], s.volname, ino, sizeStr,
		proto.StorageClass_Replica_HDD, proto.StorageClass_Replica_HDD, proto.StorageClass_Replica_HDD,
		s3key, checksum)

	resp, herr := ossAccelHTTPClient.Get(url)
	if herr != nil {
		log.LogErrorf("ossAccelColdReadGate: ino(%v) s3key(%v) mover request err: %v", ino, s3key, herr)
		return syscall.EIO, false
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	switch resp.StatusCode {
	case http.StatusOK:
		log.LogInfof("ossAccelColdReadGate: recall success ino(%v) s3key(%v) resp(%v)", ino, s3key, string(body))
		s.ic.Delete(ino) // force the caller to re-fetch StorageClass on next InodeGet
		return nil, true
	case http.StatusTooEarly:
		log.LogWarnf("ossAccelColdReadGate: recall not yet safe ino(%v) s3key(%v): %v", ino, s3key, string(body))
		return syscall.EAGAIN, false
	default:
		log.LogErrorf("ossAccelColdReadGate: recall failed ino(%v) s3key(%v) status(%v): %v", ino, s3key, resp.StatusCode, string(body))
		return syscall.EIO, false
	}
}
