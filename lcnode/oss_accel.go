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
	"encoding/json"
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

// loadOssAccelS3Config builds the s3 backend config for vol. Checks the
// volume's own per-vol override first (proto.XAttrKeyOSSAccelBackendConfig on
// proto.RootIno — see proto/oss_accel.go), falling back to the lcnode
// process's global OSS_ACCEL_S3_* environment when the volume has no
// override (zero behavior change for every deployment that never sets one).
//
// A volume WITH an override that fails to parse or is missing a required
// field is a hard error, never a silent fallback to global config — a
// misconfigured per-vol override must not quietly start writing to the
// wrong (deployment-global) bucket.
func loadOssAccelS3Config(mw *meta.MetaWrapper, vol string) (*s3.Config, error) {
	if cfg, err := loadOssAccelS3ConfigFromVol(mw); cfg != nil || err != nil {
		return cfg, err
	}
	return loadOssAccelS3ConfigFromEnv()
}

// loadOssAccelS3ConfigFromVol returns (nil, nil) when the volume has no
// oss-accel.backend override on its root inode (the normal, unconfigured
// case — caller falls back to global env). Returns (nil, non-nil error) when
// an override is present but malformed/incomplete — caller must NOT fall
// back in that case. Returns (non-nil, nil) on a valid override.
func loadOssAccelS3ConfigFromVol(mw *meta.MetaWrapper) (*s3.Config, error) {
	xattrs, err := mw.BatchGetXAttr([]uint64{proto.RootIno}, []string{proto.XAttrKeyOSSAccelBackendConfig})
	if err != nil || len(xattrs) == 0 {
		return nil, nil
	}
	raw := xattrs[0].XAttrs[proto.XAttrKeyOSSAccelBackendConfig]
	if raw == "" {
		return nil, nil
	}
	var cfg proto.OSSAccelBackendConfig
	if jerr := json.Unmarshal([]byte(raw), &cfg); jerr != nil {
		return nil, fmt.Errorf("oss-accel per-vol backend override on root inode is not valid JSON: %v", jerr)
	}
	if cfg.Endpoint == "" || cfg.Bucket == "" {
		return nil, fmt.Errorf("oss-accel per-vol backend override missing required field(s): endpoint=%q bucket=%q", cfg.Endpoint, cfg.Bucket)
	}
	region := cfg.Region
	if region == "" {
		region = "us-east-1"
	}
	accessKeyEnv := cfg.AccessKeyEnv
	if accessKeyEnv == "" {
		accessKeyEnv = envNameOssAccelS3AK
	}
	secretKeyEnv := cfg.SecretKeyEnv
	if secretKeyEnv == "" {
		secretKeyEnv = envNameOssAccelS3SK
	}
	return &s3.Config{
		Endpoint:           cfg.Endpoint,
		Region:             region,
		Bucket:             cfg.Bucket,
		AccessKeyEnv:       accessKeyEnv,
		SecretKeyEnv:       secretKeyEnv,
		UsePathStyle:       cfg.PathStyle,
		InsecureSkipVerify: cfg.SkipTLSVerify,
	}, nil
}

// loadOssAccelS3ConfigFromEnv is the pre-existing global-config path
// (deployment-wide OSS_ACCEL_S3_* env vars), unchanged.
func loadOssAccelS3ConfigFromEnv() (*s3.Config, error) {
	endpoint := os.Getenv(envOssAccelS3Endpoint)
	bucket := os.Getenv(envOssAccelS3Bucket)
	region := os.Getenv(envOssAccelS3Region)
	if endpoint == "" || bucket == "" {
		return nil, fmt.Errorf("oss-accel S3 not configured: set %s and %s in lcnode env, or set %s on the volume's root inode",
			envOssAccelS3Endpoint, envOssAccelS3Bucket, proto.XAttrKeyOSSAccelBackendConfig)
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

	metaWrapper, extentClient, err := l.buildVolClients(vol, vsc, asc)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer metaWrapper.Close()
	defer extentClient.Close()

	// Per-vol backend override lives on this vol's own root inode, so the S3
	// config can only be resolved once metaWrapper (above) exists.
	s3Cfg, err := loadOssAccelS3Config(metaWrapper, vol)
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
// Pulls the object bytes from external S3 and materializes them on the target
// inode, verifying the whole-file sha256 against the expected checksum, then
// (when the inode is currently cold) flips StorageClass back to the replica
// tier so the file is transparently readable again.
//
// Write path depends on the inode's CURRENT StorageClass (read via InodeGet_ll,
// not trusted from the request):
//   - Already a replica (e.g. a fresh empty inode used for isolated recall
//     testing): plain write (isMigration=false) — AppendExtents works normally.
//   - Still BlobStore (the real recall-to-resident case): AppendExtents no-ops
//     for BlobStore-class inodes (metanode/inode.go), so bytes MUST go through
//     the migration-write path (isMigration=true — recorded into
//     HybridCloudExtentsMigration, not HybridCloudExtents) and only become
//     visible once UpdateExtentKeyAfterMigration atomically swaps them in and
//     flips StorageClass back.
//
// Pre-check (BlobStore case only): HybridCloudExtentsMigration is the SAME slot
// used by commit-cold to stage the old replica extents for delayed release. If
// that release hasn't completed yet (info.HasMigrationEk), a migration-write now
// would append alongside the still-present old extents rather than replacing
// them — a real data-corruption risk. So recall refuses early (425) rather than
// racing the delayed-release window; callers (the client cold-read gate) are
// expected to surface a clear retryable error, not spin internally.
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
	_, vsc, asc, err := parseStorageClassForm(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	wantChecksum := strings.TrimPrefix(r.FormValue("checksum"), proto.ChecksumPrefixSHA256)

	metaWrapper, extentClient, err := l.buildVolClients(vol, vsc, asc)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer metaWrapper.Close()
	defer extentClient.Close()

	// Per-vol backend override lives on this vol's own root inode, so the S3
	// config can only be resolved once metaWrapper (above) exists.
	s3Cfg, err := loadOssAccelS3Config(metaWrapper, vol)
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

	before, gerr := metaWrapper.InodeGet_ll(ino)
	if gerr != nil || before == nil {
		http.Error(w, fmt.Sprintf("InodeGet_ll err: %v", gerr), http.StatusInternalServerError)
		return
	}
	isCold := before.StorageClass == proto.StorageClass_BlobStore
	if isCold && before.HasMigrationEk {
		if before.MigrationExtentKeyExpiredTime.After(time.Now()) {
			// Legitimate staged old-data still within its grace period (commit-cold's
			// UpdateExtentKeyAfterMigration outer handler always sets a real future
			// expiredTime). A migration-write now would append alongside it rather
			// than replacing it — a real data-corruption risk. Refuse; the caller
			// (client cold-read gate) surfaces a retryable error.
			http.Error(w, fmt.Sprintf(
				"not yet safe to recall: inode(%v) still has a pending delayed-release migration slot (from a prior tier-out); retry once its grace period ends (expires %v)",
				ino, before.MigrationExtentKeyExpiredTime), http.StatusTooEarly)
			return
		}
		// expiredTime <= now with a non-empty slot means the slot is safe to
		// discard, covering two distinct cases:
		//   - expiredTime==0: an orphaned migration-write. A previous recall
		//     attempt's isMigration writes landed in HybridCloudExtentsMigration,
		//     but its final UpdateExtentKeyAfterMigration commit never ran
		//     (crash, bug, network partition) — so it was never queued for
		//     background cleanup either (that only happens inside a successful
		//     swap's mp.freeHybridList.Push).
		//   - expiredTime in the past: legitimate staged old-data whose grace
		//     period has genuinely elapsed, but never got processed because
		//     mp.freeHybridList/mp.freeList are IN-MEMORY ONLY (metanode/free_list.go)
		//     — any metanode restart (a routine rolling deploy, not just a
		//     crash) wipes the queue, silently orphaning every inode still
		//     awaiting its 30-minute checkHybridMigrationInode sweep. Waiting
		//     longer never helps in this case; the queue entry is gone for good.
		// Either way it's safe to discard: nothing live still references this
		// slot, and the real bytes remain safe in S3 (this is the recall/
		// commit-hot direction). DeleteMigrationExtentKey unconditionally clears
		// the slot and queues its extents for real release, then we fall through
		// and retry the recall from scratch.
		log.LogWarnf("ossAccelRecall: discarding expired/orphaned migration slot (expiredTime=%v, now=%v) ino(%v)",
			before.MigrationExtentKeyExpiredTime, time.Now(), ino)
		if derr := metaWrapper.DeleteMigrationExtentKey(ino, path); derr != nil {
			http.Error(w, fmt.Sprintf("failed to discard expired/orphaned migration slot: %v", derr), http.StatusInternalServerError)
			return
		}
	}
	// writeStorageClass/isMigration: BlobStore case writes into the migration slot
	// (target = vsc, the volume's replica class) and gets swapped in atomically by
	// the commit call below. Already-replica case writes in place as usual.
	writeStorageClass := before.StorageClass
	isMigration := false
	if isCold {
		writeStorageClass = vsc
		isMigration = true
	}

	if err = extentClient.OpenStream(ino, false, false, ""); err != nil {
		http.Error(w, fmt.Sprintf("OpenStream err: %v", err), http.StatusBadRequest)
		return
	}
	defer extentClient.CloseStream(ino)

	s3key := normalizeOssAccelKey(path)

	// The whole recall (S3 GET, migration-write, checksum verify, commit) is
	// unguarded against a concurrent caller doing the exact same thing for the
	// same inode — no distributed lock, by design (see design doc). Two
	// callers racing this way isn't rare data corruption: metanode explicitly
	// detects a conflicting concurrent extent append and rejects the loser's
	// write with StatusConflictExtents (surfaces here as a plain "operation
	// not supported" error), and separately rejects a losing commit with
	// OpNotPerm once the winner's swap already landed. Either rejection means
	// the SAME thing: someone else is doing (or already did) this recall for
	// us. But the loser's write can be rejected within the first append
	// (well under a second) while the winner's own S3-GET-and-write is still
	// running (multiple seconds for a large file) — a single immediate
	// recheck right after the loser's error would still see the pre-recall
	// StorageClass and wrongly conclude this is a real failure. So on ANY
	// error in this block, poll (bounded) for the winner to finish rather
	// than checking once.
	off, got, recallErr := runOssAccelRecallWrite(extentClient, s3Backend, ino, s3key, size, writeStorageClass, isMigration, wantChecksum)
	if recallErr != nil && isCold && waitForConcurrentRecallWinner(metaWrapper, ino, vsc) {
		log.LogWarnf("ossAccelRecall: lost a concurrent recall race (%v) but ino(%v) reached StorageClass(%v) — treating as success",
			recallErr, ino, vsc)
		recallErr = nil
	}
	if recallErr != nil {
		http.Error(w, recallErr.Error(), http.StatusInternalServerError)
		return
	}

	if isCold {
		// Atomically swap the migration-write bytes into HybridCloudExtents and
		// flip StorageClass back to the replica tier. No grace period needed —
		// the swapped-out slot holds the (empty) BlobStore ObjExtents, not real data.
		if cerr := metaWrapper.UpdateExtentKeyAfterMigration(ino, vsc, nil, before.LeaseExpireTime, 0, path, true, 0); cerr != nil {
			if waitForConcurrentRecallWinner(metaWrapper, ino, vsc) {
				log.LogWarnf("ossAccelRecall: commit lost a concurrent recall race (commit err: %v) but ino(%v) reached StorageClass(%v) — treating as success",
					cerr, ino, vsc)
			} else {
				http.Error(w, fmt.Sprintf("commit-hot UpdateExtentKeyAfterMigration err: %v", cerr), http.StatusInternalServerError)
				return
			}
		}
	}

	log.LogInfof("ossAccelRecall success: vol(%v) ino(%v) written(%v) s3key(%v) sha256(%v) wasCold(%v)",
		vol, ino, off, s3key, got, isCold)
	fmt.Fprintf(w, "ok: vol=%v ino=%v written=%v s3key=%v checksum=sha256:%v wasCold=%v\n", vol, ino, off, s3key, got, isCold)
}

// concurrentRecallWaitBound is how long a losing recall waits for a concurrent
// winner to finish its own S3-GET-and-write before giving up and reporting a
// real failure. Real-machine testing showed recall latency for the same file
// size varies far more than expected under network contention (a 64MB recall
// observed anywhere from ~11s to ~255s depending on S3 path conditions,
// worsened by two concurrent downloads of the same object competing for
// bandwidth) — a short fixed bound like 60s is not a reliable margin. Matched
// instead to the client cold-read gate's own HTTP timeout to the mover
// (ossAccelHTTPClient, client/fs/oss_accel.go, 5 minutes) minus a safety
// margin: the caller is already prepared to wait that long for a response,
// so the mover should use nearly all of that budget deciding "real failure"
// vs "still waiting on a slower concurrent winner" rather than giving up
// early and forcing a spurious error the caller didn't need to see yet.
// This is still fundamentally a bounded best-effort wait, not a lock — under
// sufficiently extreme network degradation beyond what a 5-minute client
// timeout is designed to tolerate, a losing recall can still surface a
// genuine (rare) error. That is an accepted tradeoff of not introducing a
// distributed lock (see design doc), not a bug in this wait logic.
const concurrentRecallWaitBound = 4 * time.Minute

// waitForConcurrentRecallWinner polls the inode until it reaches storageClass
// vsc (a concurrent recall winner's commit landed) or concurrentRecallWaitBound
// elapses. No new coordination primitive — just repeated InodeGet_ll calls,
// deliberately not a lock/singleflight (see design doc).
func waitForConcurrentRecallWinner(metaWrapper *meta.MetaWrapper, ino uint64, vsc uint32) bool {
	deadline := time.Now().Add(concurrentRecallWaitBound)
	for {
		if after, err := metaWrapper.InodeGet_ll(ino); err == nil && after != nil && after.StorageClass == vsc {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(300 * time.Millisecond)
	}
}

// runOssAccelRecallWrite streams the S3 object into the inode's extents (S3 GET →
// ExtentClient.Write, migration-write when isCold), verifying the whole-file sha256
// against wantChecksum along the way. Returns the bytes written and the computed
// checksum even on error (best-effort, for logging) — callers decide whether an
// error here is real or just "lost a concurrent recall race" (see call site).
func runOssAccelRecallWrite(extentClient *stream.ExtentClient, s3Backend backend.Backend, ino uint64, s3key string, size uint64, writeStorageClass uint32, isMigration bool, wantChecksum string) (off int, checksum string, err error) {
	ctx := context.Background()
	body, getErr := s3Backend.Get(ctx, s3key, 0, int64(size))
	if getErr != nil {
		return 0, "", fmt.Errorf("s3 get err: %v", getErr)
	}
	defer body.Close()

	h := sha256.New()
	buf := make([]byte, 4*1024*1024)
	for {
		n, rerr := body.Read(buf)
		if n > 0 {
			wn, werr := extentClient.Write(ino, off, buf[:n], 0, nil, writeStorageClass, isMigration, false)
			if werr != nil {
				return off, hex.EncodeToString(h.Sum(nil)), fmt.Errorf("extent write err at off(%v): %v", off, werr)
			}
			h.Write(buf[:n])
			off += wn
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			return off, hex.EncodeToString(h.Sum(nil)), fmt.Errorf("s3 read err: %v", rerr)
		}
	}
	if ferr := extentClient.Flush(ino); ferr != nil {
		return off, hex.EncodeToString(h.Sum(nil)), fmt.Errorf("extent flush err: %v", ferr)
	}

	checksum = hex.EncodeToString(h.Sum(nil))
	if wantChecksum != "" && checksum != wantChecksum {
		return off, checksum, fmt.Errorf("checksum mismatch: recalled(sha256:%v) expected(sha256:%v)", checksum, wantChecksum)
	}
	return off, checksum, nil
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
	if err = metaWrapper.UpdateExtentKeyAfterMigration(ino, proto.StorageClass_BlobStore, nil, before.LeaseExpireTime, delayDelMinute, path, true, 0); err != nil {
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
