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
// S3 interface). Backend-specific connection config (endpoint/AK/SK) is NOT here
// — it lives on the mover/server side only and never reaches the client.

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
)
