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

// OSS accelerator mover: tier-out (flush) of a hot file to the external S3
// cold tier. M1 first slice (64a): upload bytes + write cold-reference xattrs.
// It does NOT yet flip the inode StorageClass or release the local extent —
// that (destructive) step is a later slice (metanode UpdateExtentKeyAfterMigration
// adaptation). So a flush here is purely additive: the file stays fully readable
// from the hot layer, and a copy now also exists in the S3 bucket under a
// path-key identical to its POSIX path (three-view consistency anchor).
//
// The S3 leaf reuses the production syncnode S3 backend (aws-sdk-go-v2, multipart
// + sha256), rather than a new S3 client. Credentials are read from the lcnode
// process environment (never from a config file, never from the request URL).

package lcnode

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data/stream"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/syncnode/backend"
	"github.com/cubefs/cubefs/syncnode/backend/s3"
	"github.com/cubefs/cubefs/util/log"
)

// Environment variables carrying the mover-side external S3 cold-tier config.
// Kept out of any committed config file; injected into the lcnode pod (k8s
// secret / daemonset env). The AK/SK are read by the s3 backend itself via the
// *Env fields below, so they never pass through this package's variables.
const (
	envOssAccelS3Endpoint      = "OSS_ACCEL_S3_ENDPOINT"
	envOssAccelS3Region        = "OSS_ACCEL_S3_REGION"
	envOssAccelS3Bucket        = "OSS_ACCEL_S3_BUCKET"
	envOssAccelS3PathStyle     = "OSS_ACCEL_S3_PATH_STYLE" // "true" for MinIO/Ceph RGW
	envOssAccelS3SkipTLSVerify = "OSS_ACCEL_S3_SKIP_TLS_VERIFY"
	// Names of the env vars holding the credentials (indirection keeps the
	// secret values inside the s3 backend, out of this package).
	envNameOssAccelS3AK = "OSS_ACCEL_S3_AK"
	envNameOssAccelS3SK = "OSS_ACCEL_S3_SK"
)

// loadOssAccelS3Config builds the s3 backend config from the lcnode environment.
// Returns an error (not a panic) when the cold backend is unconfigured so the
// flush endpoint fails cleanly rather than the daemon refusing to start.
func loadOssAccelS3Config() (*s3.Config, error) {
	endpoint := os.Getenv(envOssAccelS3Endpoint)
	bucket := os.Getenv(envOssAccelS3Bucket)
	region := os.Getenv(envOssAccelS3Region)
	if endpoint == "" || bucket == "" {
		return nil, fmt.Errorf("oss-accel S3 not configured: set %s and %s in lcnode env",
			envOssAccelS3Endpoint, envOssAccelS3Bucket)
	}
	if region == "" {
		region = "us-east-1" // harmless default for S3-compatible stores
	}
	return &s3.Config{
		Endpoint:           endpoint,
		Region:             region,
		Bucket:             bucket,
		AccessKeyEnv:       envNameOssAccelS3AK,
		SecretKeyEnv:       envNameOssAccelS3SK,
		UsePathStyle:       os.Getenv(envOssAccelS3PathStyle) == "true",
		InsecureSkipVerify: os.Getenv(envOssAccelS3SkipTLSVerify) == "true",
	}, nil
}

// normalizeOssAccelKey maps a POSIX path to the S3 object key. The key equals
// the path with the leading slash stripped, matching the objectnode key
// convention (bucket-relative, forward slashes) so the three views (POSIX /
// CubeFS-S3 / cloud direct-read) resolve to the same object.
func normalizeOssAccelKey(path string) string {
	return strings.TrimPrefix(path, "/")
}

// httpServiceOssAccelFlush handles GET /ossAccelFlush?vol=&ino=&size=&sc=&vsc=&asc=&path=
// M1 manual trigger (no scheduler yet): stream the file's hot-layer bytes to the
// external S3 bucket at its path-key, then record the cold reference in xattrs.
// Non-destructive (64a): does not flip StorageClass or release the local extent.
func (l *LcNode) httpServiceOssAccelFlush(w http.ResponseWriter, r *http.Request) {
	var err error
	if err = r.ParseForm(); err != nil {
		http.Error(w, fmt.Sprintf("ParseForm err: %v", err), http.StatusBadRequest)
		return
	}
	vol := r.FormValue("vol")
	path := r.FormValue("path")
	if vol == "" || path == "" {
		http.Error(w, "missing required form value: vol and path", http.StatusBadRequest)
		return
	}
	var ino uint64
	if ino, err = strconv.ParseUint(r.FormValue("ino"), 10, 64); err != nil {
		http.Error(w, fmt.Sprintf("ParseUint ino err: %v", err), http.StatusBadRequest)
		return
	}
	var size uint64
	if size, err = strconv.ParseUint(r.FormValue("size"), 10, 64); err != nil {
		http.Error(w, fmt.Sprintf("ParseUint size err: %v", err), http.StatusBadRequest)
		return
	}
	sc, vsc, asc, err := parseStorageClassForm(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	s3Cfg, err := loadOssAccelS3Config()
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

	metaWrapper, extentClient, err := l.buildVolClients(vol, vsc, asc)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer metaWrapper.Close()
	defer extentClient.Close()

	if err = extentClient.OpenStream(ino, false, false, ""); err != nil {
		http.Error(w, fmt.Sprintf("OpenStream err: %v", err), http.StatusBadRequest)
		return
	}
	defer extentClient.CloseStream(ino)

	t := &TransitionMgr{ec: extentClient, ecForW: extentClient}
	e := &proto.ScanDentry{Size: size, Inode: ino, StorageClass: sc}
	s3key := normalizeOssAccelKey(path)

	// Stream hot-layer bytes → S3. The backend computes the whole-file sha256
	// alongside the upload (ComputeChecksum) and handles multipart internally.
	pr, pw := io.Pipe()
	var srcErr error
	go func() {
		srcErr = t.readFromExtentClient(e, pw, false, 0, 0)
		pw.CloseWithError(srcErr)
	}()

	ctx := context.Background()
	putRes, putErr := s3Backend.Put(ctx, s3key, pr, int64(size), backend.PutOptions{ComputeChecksum: true})
	pr.Close()
	if srcErr != nil {
		http.Error(w, fmt.Sprintf("read src extent err: %v", srcErr), http.StatusInternalServerError)
		return
	}
	if putErr != nil {
		http.Error(w, fmt.Sprintf("s3 put err: %v", putErr), http.StatusInternalServerError)
		return
	}

	// Two-phase check: HEAD the freshly written object and verify size.
	headSize, _, _, headErr := s3Backend.Head(ctx, s3key)
	if headErr != nil {
		http.Error(w, fmt.Sprintf("s3 head verify err: %v", headErr), http.StatusInternalServerError)
		return
	}
	if headSize != int64(size) {
		http.Error(w, fmt.Sprintf("s3 size mismatch: local(%v) remote(%v)", size, headSize), http.StatusInternalServerError)
		return
	}

	// Record the cold reference in xattrs (extendTree). State stays clean; the
	// inode remains a hot Replica until a later slice flips the class.
	checksum := proto.ChecksumPrefixSHA256 + putRes.Checksum
	attrs := map[string]string{
		proto.XAttrKeyOSSAccelS3Key:    s3key,
		proto.XAttrKeyOSSAccelChecksum: checksum,
		proto.XAttrKeyOSSAccelSize:     strconv.FormatUint(size, 10),
		proto.XAttrKeyOSSAccelState:    proto.ColdStateClean,
	}
	if err = metaWrapper.BatchSetXAttr_ll(ino, attrs); err != nil {
		http.Error(w, fmt.Sprintf("set oss-accel xattr err: %v", err), http.StatusInternalServerError)
		return
	}

	log.LogInfof("ossAccelFlush success: vol(%v) ino(%v) size(%v) s3key(%v) checksum(%v)",
		vol, ino, size, s3key, checksum)
	fmt.Fprintf(w, "ok: vol=%v ino=%v size=%v s3key=%v checksum=%v\n", vol, ino, size, s3key, checksum)
}

// buildVolClients constructs a meta + extent client pair for a volume, shared by
// the mover flush / recall endpoints (mirrors httpServiceGetFile setup). Callers
// defer Close() on both returned clients.
func (l *LcNode) buildVolClients(vol string, vsc uint32, asc []uint32) (*meta.MetaWrapper, *stream.ExtentClient, error) {
	metaWrapper, err := meta.NewMetaWrapper(&meta.MetaConfig{
		Volume:               vol,
		Masters:              l.masters,
		Authenticate:         false,
		ValidateOwner:        false,
		InnerReq:             true,
		MetaSendTimeout:      600,
		DisableTrashByClient: true,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("NewMetaWrapper err: %v", err)
	}
	extentClient, err := stream.NewExtentClient(&stream.ExtentConfig{
		Volume:                      vol,
		Masters:                     l.masters,
		OnAppendExtentKey:           metaWrapper.AppendExtentKey,
		OnSplitExtentKey:            metaWrapper.SplitExtentKey,
		OnGetExtents:                metaWrapper.GetExtents,
		OnTruncate:                  metaWrapper.Truncate,
		OnRenewalForbiddenMigration: metaWrapper.RenewalForbiddenMigration,
		VolStorageClass:             vsc,
		VolAllowedStorageClass:      asc,
		OnForbiddenMigration:        metaWrapper.ForbiddenMigration,
		InnerReq:                    true,
		MetaWrapper:                 metaWrapper,
	})
	if err != nil {
		metaWrapper.Close()
		return nil, nil, fmt.Errorf("NewExtentClient err: %v", err)
	}
	return metaWrapper, extentClient, nil
}

// httpServiceOssAccelRecall handles GET /ossAccelRecall?vol=&ino=&size=&sc=&vsc=&asc=&path=&checksum=
// M1 recall data leaf (isolated): pull the object bytes from external S3 and write
// them into the target inode's extents, verifying the whole-file sha256 against the
// expected checksum. This is the materialize-from-S3 half of recall-to-resident; it
// does NOT yet flip StorageClass or drive the client read path (later slices). For
// safe isolated testing, recall into a fresh (empty) inode.
func (l *LcNode) httpServiceOssAccelRecall(w http.ResponseWriter, r *http.Request) {
	var err error
	if err = r.ParseForm(); err != nil {
		http.Error(w, fmt.Sprintf("ParseForm err: %v", err), http.StatusBadRequest)
		return
	}
	vol := r.FormValue("vol")
	path := r.FormValue("path")
	if vol == "" || path == "" {
		http.Error(w, "missing required form value: vol and path", http.StatusBadRequest)
		return
	}
	var ino uint64
	if ino, err = strconv.ParseUint(r.FormValue("ino"), 10, 64); err != nil {
		http.Error(w, fmt.Sprintf("ParseUint ino err: %v", err), http.StatusBadRequest)
		return
	}
	var size uint64
	if size, err = strconv.ParseUint(r.FormValue("size"), 10, 64); err != nil {
		http.Error(w, fmt.Sprintf("ParseUint size err: %v", err), http.StatusBadRequest)
		return
	}
	sc, vsc, asc, err := parseStorageClassForm(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	wantChecksum := strings.TrimPrefix(r.FormValue("checksum"), proto.ChecksumPrefixSHA256)

	s3Cfg, err := loadOssAccelS3Config()
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

	metaWrapper, extentClient, err := l.buildVolClients(vol, vsc, asc)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer metaWrapper.Close()
	defer extentClient.Close()

	if err = extentClient.OpenStream(ino, false, false, ""); err != nil {
		http.Error(w, fmt.Sprintf("OpenStream err: %v", err), http.StatusBadRequest)
		return
	}
	defer extentClient.CloseStream(ino)

	s3key := normalizeOssAccelKey(path)
	ctx := context.Background()
	body, getErr := s3Backend.Get(ctx, s3key, 0, int64(size))
	if getErr != nil {
		http.Error(w, fmt.Sprintf("s3 get err: %v", getErr), http.StatusInternalServerError)
		return
	}
	defer body.Close()

	// Stream S3 → extents; compute sha256 alongside; verify against expected.
	h := sha256.New()
	buf := make([]byte, 4*1024*1024)
	var off int
	for {
		n, rerr := body.Read(buf)
		if n > 0 {
			wn, werr := extentClient.Write(ino, off, buf[:n], 0, nil, sc, false, false)
			if werr != nil {
				http.Error(w, fmt.Sprintf("extent write err at off(%v): %v", off, werr), http.StatusInternalServerError)
				return
			}
			h.Write(buf[:n])
			off += wn
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			http.Error(w, fmt.Sprintf("s3 read err: %v", rerr), http.StatusInternalServerError)
			return
		}
	}
	if err = extentClient.Flush(ino); err != nil {
		http.Error(w, fmt.Sprintf("extent flush err: %v", err), http.StatusInternalServerError)
		return
	}

	got := hex.EncodeToString(h.Sum(nil))
	if wantChecksum != "" && got != wantChecksum {
		http.Error(w, fmt.Sprintf("checksum mismatch: recalled(sha256:%v) expected(sha256:%v)", got, wantChecksum), http.StatusInternalServerError)
		return
	}

	log.LogInfof("ossAccelRecall success: vol(%v) ino(%v) written(%v) s3key(%v) sha256(%v)",
		vol, ino, off, s3key, got)
	fmt.Fprintf(w, "ok: vol=%v ino=%v written=%v s3key=%v checksum=sha256:%v\n", vol, ino, off, s3key, got)
}

// httpServiceOssAccelCommitCold handles GET /ossAccelCommitCold?vol=&ino=&sc=&vsc=&asc=&path=&delayDelMinute=&leaseExpire=
// Flips a previously-flushed inode to the cold tier: StorageClass=BlobStore + empty
// ObjExtents (the bytes live in external S3, referenced by the oss-accel xattrs),
// staging the old replica extents into the migration slot for delayed release
// (grace period = delayDelMinute). DESTRUCTIVE after the grace period: local extents
// are freed. Safe because the bytes are already durable in S3 (verified by a prior
// flush) and recoverable via /ossAccelRecall.
//
// Reuses the existing metanode UpdateExtentKeyAfterMigration op, which already accepts
// StorageClass_BlobStore with nil ObjExtentKeys (→ empty obj-extents). leaseExpire is
// the inode's current migration-lease generation (0 when no lease is held).
func (l *LcNode) httpServiceOssAccelCommitCold(w http.ResponseWriter, r *http.Request) {
	var err error
	if err = r.ParseForm(); err != nil {
		http.Error(w, fmt.Sprintf("ParseForm err: %v", err), http.StatusBadRequest)
		return
	}
	vol := r.FormValue("vol")
	path := r.FormValue("path")
	if vol == "" || path == "" {
		http.Error(w, "missing required form value: vol and path", http.StatusBadRequest)
		return
	}
	var ino uint64
	if ino, err = strconv.ParseUint(r.FormValue("ino"), 10, 64); err != nil {
		http.Error(w, fmt.Sprintf("ParseUint ino err: %v", err), http.StatusBadRequest)
		return
	}
	_, vsc, asc, err := parseStorageClassForm(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	delayDelMinute := parseUintForm(r, "delayDelMinute", 0)

	metaWrapper, extentClient, err := l.buildVolClients(vol, vsc, asc)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer metaWrapper.Close()
	defer extentClient.Close()

	before, gerr := metaWrapper.InodeGet_ll(ino)
	if gerr != nil || before == nil {
		http.Error(w, fmt.Sprintf("InodeGet_ll err: %v", gerr), http.StatusInternalServerError)
		return
	}
	// Migration is forbidden while the write-migration lease is still valid (a file
	// written within ForbiddenMigrationRenewalSeonds=3600s carries a lease). Refuse
	// early with a clear message rather than a raw metanode 500, and pass the inode's
	// current lease generation so the metanode generation check matches.
	now := uint64(time.Now().Unix())
	if before.LeaseExpireTime >= now {
		http.Error(w, fmt.Sprintf("migration lease not expired: inode(%v) leaseExpireTime(%v) >= now(%v); file written recently, retry after ~%vs",
			ino, before.LeaseExpireTime, now, before.LeaseExpireTime-now), http.StatusConflict)
		return
	}
	if err = metaWrapper.UpdateExtentKeyAfterMigration(ino, proto.StorageClass_BlobStore, nil, before.LeaseExpireTime, delayDelMinute, path); err != nil {
		http.Error(w, fmt.Sprintf("UpdateExtentKeyAfterMigration err: %v", err), http.StatusInternalServerError)
		return
	}
	after, aerr := metaWrapper.InodeGet_ll(ino)

	var beforeSC, afterSC, afterMig uint32
	var afterSize uint64
	if before != nil {
		beforeSC = before.StorageClass
	}
	if aerr == nil && after != nil {
		afterSC, afterMig, afterSize = after.StorageClass, after.MigrationStorageClass, after.Size
	}
	log.LogInfof("ossAccelCommitCold: vol(%v) ino(%v) storageClass %v->%v migrationSC(%v) size(%v) grace(%vmin)",
		vol, ino, beforeSC, afterSC, afterMig, afterSize, delayDelMinute)
	fmt.Fprintf(w, "ok: vol=%v ino=%v storageClass=%v->%v migrationStorageClass=%v size=%v graceMinute=%v\n",
		vol, ino, beforeSC, afterSC, afterMig, afterSize, delayDelMinute)
}

// parseUintForm parses an optional uint64 form value, returning def when absent/empty.
func parseUintForm(r *http.Request, key string, def uint64) uint64 {
	s := r.FormValue(key)
	if s == "" {
		return def
	}
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return def
	}
	return v
}

// parseStorageClassForm parses sc/vsc/asc form values shared by the mover
// endpoints (same encoding as httpServiceGetFile).
func parseStorageClassForm(r *http.Request) (sc, vsc uint32, asc []uint32, err error) {
	var v uint64
	if v, err = strconv.ParseUint(r.FormValue("sc"), 10, 32); err != nil {
		return 0, 0, nil, fmt.Errorf("ParseUint sc err: %v", err)
	}
	sc = uint32(v)
	if v, err = strconv.ParseUint(r.FormValue("vsc"), 10, 32); err != nil {
		return 0, 0, nil, fmt.Errorf("ParseUint vsc err: %v", err)
	}
	vsc = uint32(v)
	for _, scStr := range strings.Split(r.FormValue("asc"), ",") {
		if scStr == "" {
			continue
		}
		if v, err = strconv.ParseUint(scStr, 10, 32); err != nil {
			return 0, 0, nil, fmt.Errorf("ParseUint asc err: %v", err)
		}
		asc = append(asc, uint32(v))
	}
	return sc, vsc, asc, nil
}
