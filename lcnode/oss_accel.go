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
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"

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

	// Meta + extent client for the volume, mirroring httpServiceGetFile.
	metaConfig := &meta.MetaConfig{
		Volume:               vol,
		Masters:              l.masters,
		Authenticate:         false,
		ValidateOwner:        false,
		InnerReq:             true,
		MetaSendTimeout:      600,
		DisableTrashByClient: true,
	}
	metaWrapper, err := meta.NewMetaWrapper(metaConfig)
	if err != nil {
		http.Error(w, fmt.Sprintf("NewMetaWrapper err: %v", err), http.StatusBadRequest)
		return
	}
	defer metaWrapper.Close()

	extentConfig := &stream.ExtentConfig{
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
	}
	extentClient, err := stream.NewExtentClient(extentConfig)
	if err != nil {
		http.Error(w, fmt.Sprintf("NewExtentClient err: %v", err), http.StatusBadRequest)
		return
	}
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
