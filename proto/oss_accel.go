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

package proto

// OSS accelerator (object-store accelerator) shared protocol constants.
//
// Design: the cloud object store is the persistent/trust tier (no self-hosted
// blobstore); CubeFS is a dual-protocol (POSIX + S3) accelerating cache on top.
// A cold file keeps StorageClass=BlobStore in metanode but its bytes live in the
// external S3 bucket under a path-key identical to its POSIX path / objectnode
// key (three-view consistency). The cold reference and integrity metadata are
// carried in independent xattrs (extendTree), leaving the inode binary format
// untouched.
//
// These constants are shared across mover (lcnode, writes them), fuse/kernel
// client (reads them to route cold reads), and objectnode (reads them for the
// S3 interface). Per-inode cold-reference xattrs never carry connection config.
//
// XAttrKeyOSSAccelBackendConfig is the one exception, and it lives on a
// DIFFERENT inode (the volume root, proto.RootIno) rather than on any
// tiered-out file: an optional per-volume override of the mover's cold S3
// backend (endpoint/bucket/region/path-style/TLS + the NAMES of the env vars
// holding AK/SK — never the credential values themselves, which still only
// ever live in the lcnode process environment). Absent on a volume, the
// mover falls back to its global OSS_ACCEL_S3_* environment config
// (lcnode/oss_accel.go loadOssAccelS3Config) — zero behavior change for any
// deployment that hasn't set this.

// XAttr keys for the cold reference carried on a tiered-out inode.
// Prefix "oss-accel." is distinct from objectnode's "oss:" keys to avoid clash.
const (
	// XAttrKeyOSSAccelS3Key is the external S3 object key of the whole-file
	// object = the normalized POSIX path (three-view consistency anchor).
	XAttrKeyOSSAccelS3Key = "oss-accel.s3key"
	// XAttrKeyOSSAccelChecksum is the whole-file sha256 ("sha256:<hex>"),
	// computed independently of any multipart ETag; verified after recall.
	XAttrKeyOSSAccelChecksum = "oss-accel.checksum"
	// XAttrKeyOSSAccelGen is the inode generation at tier-out time; guards
	// against stale recall / cross-generation mixups.
	XAttrKeyOSSAccelGen = "oss-accel.gen"
	// XAttrKeyOSSAccelSize is the whole-file byte length.
	XAttrKeyOSSAccelSize = "oss-accel.size"
	// XAttrKeyOSSAccelState is the cold sub-state (see ColdState* below).
	XAttrKeyOSSAccelState = "oss-accel.state"
)

// XAttrKeyOSSAccelBackendConfig is the per-volume cold-backend override,
// stored on the VOLUME ROOT inode (proto.RootIno), not on any tiered-out
// file. See the package doc comment above for why this is the one xattr that
// carries backend config (never credentials) rather than a per-file cold
// reference.
const XAttrKeyOSSAccelBackendConfig = "oss-accel.backend"

// XAttrKeyOSSAccelChangelogCursor is the M2 (reverse acceleration) changelog
// read position, stored on the VOLUME ROOT inode alongside
// XAttrKeyOSSAccelBackendConfig. Value is a decimal uint64 byte offset into
// the volume's external changelog object (see lcnode/oss_accel.go
// httpServiceOssAccelChangelogSync) — "how far the mover has read", not an
// authoritative record of what's been materialized (that's derivable from
// the CubeFS namespace itself via idempotent Lookup_ll checks), so a lost or
// reset cursor causes at most reprocessing, never corruption.
const XAttrKeyOSSAccelChangelogCursor = "oss-accel.changelog.cursor"

// OSSAccelBackendConfig is the JSON shape stored at XAttrKeyOSSAccelBackendConfig.
// Every field mirrors the corresponding OSS_ACCEL_S3_* env var; AccessKeyEnv/
// SecretKeyEnv name the environment variables the mover reads for credentials
// (defaulting to the global OSS_ACCEL_S3_AK/OSS_ACCEL_S3_SK names when empty),
// so a volume-specific backend can still reuse the deployment's existing
// credentials, or point at differently-named env vars for a distinct
// credential pair — but the credential VALUES are never written here.
type OSSAccelBackendConfig struct {
	Endpoint      string `json:"endpoint"`
	Region        string `json:"region,omitempty"`
	Bucket        string `json:"bucket"`
	AccessKeyEnv  string `json:"accessKeyEnv,omitempty"`
	SecretKeyEnv  string `json:"secretKeyEnv,omitempty"`
	PathStyle     bool   `json:"pathStyle,omitempty"`
	SkipTLSVerify bool   `json:"skipTlsVerify,omitempty"`
	// AllowBackendCredentialAuth opts this volume in to accepting S3
	// requests against ObjectNode that are signed with THIS backend's own
	// AK/SK (the same pair used to reach the external cold bucket) as a
	// valid identity — full read/write, mapped to the volume owner's
	// permissions. Off by default: this is a bridge from a
	// pre-existing-elsewhere credential into CubeFS's own namespace, not
	// something that should activate just because a backend is configured.
	AllowBackendCredentialAuth bool `json:"allowBackendCredentialAuth,omitempty"`
	// ProfileName selects a named profile in the shared-credentials file
	// mounted into lcnode/objectnode (a "[profile-name]" section, alongside
	// the deployment's default "[default]" profile) — lets this volume
	// resolve to a genuinely different AK/SK than every other volume
	// sharing the same pod, without needing its own AccessKeyEnv/
	// SecretKeyEnv pointing at separately-injected env vars. Empty (the
	// default) resolves "[default]" exactly as before this field existed.
	ProfileName string `json:"profileName,omitempty"`
}

// ChecksumPrefixSHA256 prefixes the value stored in XAttrKeyOSSAccelChecksum.
const ChecksumPrefixSHA256 = "sha256:"

// Cold backend selection. A deployment either keeps the native blobstore cold
// tier or points the cold tier at an external S3 bucket. This is a
// deployment-level choice (not per-inode); the cold-read/cold-write leaf
// implementations dispatch on it. A file being cold is still signalled by
// StorageClass=BlobStore either way (DEC-M1-1: reuse the enum, no new value).
const (
	// ColdBackendBlobStore is the native blobstore cold tier (default;
	// preserves existing behavior).
	ColdBackendBlobStore = "blobstore"
	// ColdBackendExternalS3 routes the cold tier to an external S3 bucket
	// (the object-store accelerator mode).
	ColdBackendExternalS3 = "external_s3"
)

// ColdState is the value stored in XAttrKeyOSSAccelState.
const (
	// ColdStateClean: resident in cold tier, no recall in flight.
	ColdStateClean = "clean"
	// ColdStateRecalling: a recall is in flight; a CAS on this value prevents
	// duplicate concurrent recalls of the same inode.
	ColdStateRecalling = "recalling"
	// ColdStateError: last recall failed; readers must return a distinguishable
	// errno rather than zero-filled data.
	ColdStateError = "error"
	// ColdStateMaterialized: created by M2 changelog sync
	// (lcnode/oss_accel.go materializeOssAccelChangelogEvent) as a
	// never-written placeholder — distinct from ColdStateClean (M1's
	// tier-out of a real, previously-hot file) so the M2 收尾 TTL sweep
	// (lcnode/oss_accel_walk.go) can tell "unread placeholder, safe to
	// reclaim" apart from "legitimately cold, native-tiered file" using the
	// SAME xattr the mover already writes — no extra bookkeeping needed. A
	// successful recall flips StorageClass away from BlobStore (M1's
	// existing commit-hot path) without touching this xattr, so a
	// materialized placeholder that HAS been read is excluded from the
	// sweep automatically (its StorageClass no longer matches the sweep's
	// filter) — it is never relabeled ColdStateClean.
	ColdStateMaterialized = "materialized"
)

// XAttrKeyOSSAccelPin marks a file as excluded from any oss-accel background
// sweep (M2 收尾 TTL cleanup, M3 coldest-first eviction) — value "true".
// Checked by the shared walker (lcnode/oss_accel_walk.go) so every sweep
// respects it uniformly; absent or any other value = not pinned.
const XAttrKeyOSSAccelPin = "oss-accel.pin"

// XAttrKeyOSSAccelLastRecallTime (M3, RFC3339) is stamped on a successful
// recall (lcnode/oss_accel.go, commit-hot path) — the coldest-first eviction
// sweep's recency signal. Deliberately independent of metanode's native
// AccessTime, which requires a per-volume opt-in (EnablePersistAccessTime)
// that isn't guaranteed on every volume; oss-accel keeps its own always-on
// signal instead of depending on a setting it doesn't control.
const XAttrKeyOSSAccelLastRecallTime = "oss-accel.lastRecallTime"

// XAttrKeyOSSAccelLastIntegrityCheckTime (系统层面收尾续/补1+3, RFC3339) is
// stamped after EVERY full-tier integrity check (match or mismatch —
// lcnode/oss_accel_integrity.go), never after a cheap (HeadObject-only)
// check. The integrity sweep sorts candidates ascending by this field to
// pick its FullSampleCount sample each run, the same "oldest/never-checked
// sorts first" rotation OSSAccelEvictionRuleManager's ranking uses for
// XAttrKeyOSSAccelLastRecallTime — guarantees repeated runs eventually cover
// every cold object instead of re-checking the same few every time.
const XAttrKeyOSSAccelLastIntegrityCheckTime = "oss-accel.lastIntegrityCheckTime"

// XAttrKeyOSSAccelFlushedAt (差距分析续/漂移自动刷新, RFC3339) records when THIS
// cluster last wrote the object at XAttrKeyOSSAccelS3Key. Written by flush
// alongside the s3key/checksum/size/state group (lcnode/oss_accel.go), so it
// costs no extra RPC.
//
// It exists for exactly one purpose: to make a checksum mismatch
// INTERPRETABLE. Compared against the S3 object's own Mtime (LastModified —
// see syncnode/backend.Stat), it discriminates the two explanations a
// mismatch otherwise has no way to tell apart:
//
//	S3 Mtime  > flushedAt  →  somebody wrote the object AFTER us, i.e. a
//	                          legitimate external update → refresh our
//	                          recorded checksum/size to match.
//	S3 Mtime <= flushedAt  →  nobody wrote it since we did, yet the content
//	                          no longer matches → possible silent corruption
//	                          → mark, don't silently follow it.
//
// Absent (every cold file that predates this xattr) means the mismatch is
// UNINTERPRETABLE, and the integrity sweep must fall back to its previous
// behavior rather than guess in either direction. Such files acquire the
// field on their next flush.
//
// Note this is distinct from XAttrKeyOSSAccelLastRecallTime and
// XAttrKeyOSSAccelLastIntegrityCheckTime, which record when WE acted on the
// file; this one is a claim about the OBJECT's version lineage.
const XAttrKeyOSSAccelFlushedAt = "oss-accel.flushedAt"

// XAttrKeyOSSAccelRoleConfig is the M4 (multi-cluster) per-volume write role,
// stored on the VOLUME ROOT inode alongside XAttrKeyOSSAccelBackendConfig —
// same storage mechanism, same reasoning: a role is a deployment-level
// setting about THIS cluster's relationship to the shared bucket, not a
// per-file cold reference. Absent on a volume = OSSAccelRolePrimary with no
// restriction, i.e. zero behavior change for every volume that predates M4
// or never configures this (every M1/M2/M3 real-hardware test this session
// ran against a volume with no role xattr at all).
const XAttrKeyOSSAccelRoleConfig = "oss-accel.role"

// OSSAccelRoleConfig is the JSON shape stored at XAttrKeyOSSAccelRoleConfig.
//
// OwnedPrefixes is only consulted when Role==OSSAccelRoleSecondary: keys
// under one of these prefixes are still writable by THIS cluster (a
// secondary can be the delegated primary for a sub-scope — e.g. the
// project charter's own example, cluster B is secondary for datasets/ but
// primary for ckpt/). Empty OwnedPrefixes on a secondary blocks ALL writes
// for this volume. OwnedPrefixes is meaningless and ignored for BOTH
// OSSAccelRolePrimary (already unrestricted) and OSSAccelRoleReadOnly
// (never writable) — kept rather than validated away, since a later switch
// to secondary can reuse whatever was already configured.
//
// Prefixes are matched against the S3 KEY, which has no leading slash
// (see normalizeOssAccelKey in lcnode) — write "ckpt/", NOT "/ckpt". A
// leading slash used to make a prefix match nothing at all, silently
// blocking every write on the volume; it is now stripped on both the CLI
// write path and the lcnode read path. Matching is path-segment aware:
// "ckpt" matches the key "ckpt" and anything under "ckpt/", but NOT
// "ckptx/...".
type OSSAccelRoleConfig struct {
	Role          string   `json:"role"`
	OwnedPrefixes []string `json:"ownedPrefixes,omitempty"`
}

// OSSAccelRole values for OSSAccelRoleConfig.Role.
const (
	// OSSAccelRolePrimary: unrestricted — may write/tier-out to the shared
	// S3 backend for any path in this volume. Default when unconfigured.
	OSSAccelRolePrimary = "primary"
	// OSSAccelRoleSecondary: blocked from writing except under
	// OSSAccelRoleConfig.OwnedPrefixes — consumes via changelog tailing
	// (M2's existing httpServiceOssAccelChangelogSync, unchanged) instead.
	OSSAccelRoleSecondary = "secondary"
	// OSSAccelRoleReadOnly: the bucket belongs to an EXTERNAL system, not to
	// any CubeFS cluster. Distinct from a secondary with no OwnedPrefixes,
	// which forbids the same set of writes but means something different:
	// secondary says "another CubeFS cluster owns this prefix, the bucket is
	// still collectively ours", readonly says "nobody here owns any of it".
	//
	// That difference drives two behaviors beyond the write gate:
	//   - Destructive housekeeping (orphan quarantine, rename-drift
	//     relocate, trash purge) is refused rather than performed. Detection
	//     still runs and still reports, so an operator sees dangling
	//     references / orphan candidates / drift without oss-accel acting on
	//     someone else's bucket.
	//   - A checksum mismatch is NOT marked ColdStateError. Against an
	//     externally-written bucket a mismatch most likely means the owner
	//     legitimately updated the object, and ColdStateError is an
	//     unclearable read-block for a committed-cold file (nothing writes
	//     ColdStateClean without a hot copy to re-flush).
	OSSAccelRoleReadOnly = "readonly"
)

// XAttrKeyOSSAccelWriteThrough is the 差距分析续(同步预热) per-volume
// write-through mode, on the VOLUME ROOT inode. JSON shape
// OSSAccelWriteThroughConfig. Absent = OSSAccelWriteThroughOff, i.e. zero
// behavior change for every existing volume.
//
// Deliberately its OWN xattr rather than a field on OSSAccelBackendConfig,
// even though both are volume-root oss-accel config. Reason: LoadBackendConfig
// returns nil when oss-accel.backend is absent, and lcnode only then falls
// back to the deployment-global OSS_ACCEL_S3_* environment config. Setting the
// backend xattr merely to carry a write-through mode would force the operator
// to also fill in endpoint/bucket/region, which would OVERRIDE that env
// fallback — turning a working env-configured volume into a misconfigured one
// as a side effect of enabling write-through. Separate keys keep the two
// concerns independently settable.
const XAttrKeyOSSAccelWriteThrough = "oss-accel.writeThrough"

// OSSAccelWriteThroughConfig is the JSON shape stored at
// XAttrKeyOSSAccelWriteThrough.
type OSSAccelWriteThroughConfig struct {
	Mode string `json:"mode"`
}

// OSSAccelWriteThrough* are the modes for OSSAccelWriteThroughConfig.Mode.
//
// Scope note: write-through only applies to writes that arrive through
// CubeFS's own S3 gateway (ObjectNode). POSIX writes (kernel client / FUSE)
// are a completely separate process and data path, and a POSIX write() cannot
// wait on an S3 upload — those still reach the cold tier only via the
// scheduled flush policy. This matches how the feature works in comparable
// products, where the accelerator IS the object gateway.
const (
	// OSSAccelWriteThroughOff: no flush at write time. The object reaches the
	// cold backend only when a flush policy or a manual flush picks it up.
	// Default when unconfigured.
	OSSAccelWriteThroughOff = "off"
	// OSSAccelWriteThroughAsync: ack the PUT as soon as CubeFS has the data,
	// then flush to the cold backend in the background. PUT latency is
	// unchanged. Narrows the existing "acked but not yet in S3" window from
	// however long the flush policy's interval is (hours) down to seconds —
	// a strict improvement over off, not a new risk.
	OSSAccelWriteThroughAsync = "async"
	// OSSAccelWriteThroughSync: do not ack the PUT until the cold-backend
	// upload has succeeded; a failed upload fails the PUT. Costs roughly a
	// second full pass over the object's bytes (CubeFS write, then lcnode
	// reads it back and uploads), so PUT latency rises accordingly.
	//
	// Note this still does not make the cold copy authoritative — CubeFS's own
	// replicated tier remains the primary. It guarantees "durable in the cold
	// backend at ack time", not "the local copy is disposable".
	OSSAccelWriteThroughSync = "sync"
)

// IsValidOSSAccelWriteThroughMode reports whether mode is a known value.
// Shared by the cfs-cli setter and ObjectNode's reader so the accepted set
// cannot drift between them.
func IsValidOSSAccelWriteThroughMode(mode string) bool {
	switch mode {
	case OSSAccelWriteThroughOff, OSSAccelWriteThroughAsync, OSSAccelWriteThroughSync:
		return true
	default:
		return false
	}
}

// IsValidOSSAccelRole reports whether role is one of the three known values.
// Shared by lcnode's config loader and the cfs-cli setter so the accepted
// set can never drift between the write path and the enforcement path.
func IsValidOSSAccelRole(role string) bool {
	switch role {
	case OSSAccelRolePrimary, OSSAccelRoleSecondary, OSSAccelRoleReadOnly:
		return true
	default:
		return false
	}
}
