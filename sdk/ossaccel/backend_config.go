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

// Package ossaccel holds oss-accel logic shared by more than one server
// role. LoadBackendConfig was originally private to lcnode (the mover); it
// moved here so objectnode (the S3 gateway, for the backend-credential
// auth bridge) can read the same per-vol override without importing the
// lcnode package.
package ossaccel

import (
	"encoding/json"
	"fmt"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/meta"
)

// Default env var names used when a volume's oss-accel backend override
// doesn't specify its own — matches the deployment-wide OSS_ACCEL_S3_AK/SK
// convention.
const (
	DefaultAccessKeyEnv = "OSS_ACCEL_S3_AK"
	DefaultSecretKeyEnv = "OSS_ACCEL_S3_SK"
)

// LoadBackendConfig returns this volume's oss-accel S3 backend config
// (proto.XAttrKeyOSSAccelBackendConfig on proto.RootIno), or (nil, nil) when
// the volume has none — which is NOT a fallback case: there is no
// deployment-wide default bucket, so lcnode turns (nil, nil) into a hard
// "not configured for this volume" error. Returns (nil, non-nil error) when
// the config is present but malformed/incomplete, or when it could not be
// read at all.
//
// Region/AccessKeyEnv/SecretKeyEnv are defaulted here when the config
// doesn't specify its own, so every caller sees the same resolved values.
func LoadBackendConfig(mw *meta.MetaWrapper) (*proto.OSSAccelBackendConfig, error) {
	xattrs, err := mw.BatchGetXAttr([]uint64{proto.RootIno}, []string{proto.XAttrKeyOSSAccelBackendConfig})
	// A metanode READ FAILURE must not be reported as "no config". It used to
	// be folded in with the empty case because "no config" meant "fall back to
	// the deployment-global bucket", which was a survivable answer. Now that
	// "no config" is a hard error telling the operator to go configure the
	// volume, conflating the two would print exactly the wrong diagnostic for
	// a volume that IS configured and merely had a transient read failure.
	if err != nil {
		return nil, fmt.Errorf("oss-accel backend config read failed on vol root inode: %v", err)
	}
	// len(xattrs)==0 genuinely means "this inode has no xattrs at all" —
	// metanode only appends an XAttrInfo for inodes with an extendTree entry,
	// so this is the normal state of an unconfigured volume, not a failure.
	if len(xattrs) == 0 {
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
	if cfg.Region == "" {
		cfg.Region = "us-east-1"
	}
	if cfg.AccessKeyEnv == "" {
		cfg.AccessKeyEnv = DefaultAccessKeyEnv
	}
	if cfg.SecretKeyEnv == "" {
		cfg.SecretKeyEnv = DefaultSecretKeyEnv
	}
	return &cfg, nil
}
