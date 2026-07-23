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

// LoadBackendConfig returns this volume's oss-accel S3 backend override
// (proto.XAttrKeyOSSAccelBackendConfig on proto.RootIno), or (nil, nil) when
// the volume has no override configured — the normal, unconfigured case.
// Returns (nil, non-nil error) when an override is present but
// malformed/incomplete; callers must not silently fall back to a
// deployment-global default in that case — a misconfigured per-vol override
// must not quietly resolve to the wrong bucket/credentials.
//
// Region/AccessKeyEnv/SecretKeyEnv are defaulted here when the override
// doesn't specify its own, so every caller sees the same resolved values.
func LoadBackendConfig(mw *meta.MetaWrapper) (*proto.OSSAccelBackendConfig, error) {
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
