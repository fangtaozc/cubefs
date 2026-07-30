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
	// 凭证来源必须显式声明:要么 profileName,要么 accessKeyEnv+secretKeyEnv
	// 成对。三者都不给曾经会静默落到 AWS SDK 默认凭证链去读共享凭证文件的
	// [default] 段——那是和"部署级默认桶"同一类的隐式默认:"这个卷用的是哪套
	// 凭证"从卷自身答不出来,漏配一个卷不会报错、只会悄悄用上别人的凭证。
	//
	// 以前这里把 accessKeyEnv/secretKeyEnv 默认成 OSS_ACCEL_S3_AK/_SK,正是
	// 那条隐式路径的入口(env 不存在 -> 取到空 -> 落默认链 -> 读 [default])。
	// 现在两个默认值一并删掉:profileName 非空时这两个字段保持为空,由
	// s3.Config.Validate 承认 Profile 是合法凭证来源。
	if cfg.ProfileName == "" && (cfg.AccessKeyEnv == "" || cfg.SecretKeyEnv == "") {
		return nil, fmt.Errorf("oss-accel backend config for this volume declares no credential source: "+
			"set exactly one of `--profile <name>` (a section in the mounted shared-credentials file) "+
			"or `--access-key-env <NAME> --secret-key-env <NAME>` (both required together). "+
			"There is no implicit default credential any more — got profileName=%q accessKeyEnv=%q secretKeyEnv=%q",
			cfg.ProfileName, cfg.AccessKeyEnv, cfg.SecretKeyEnv)
	}
	return &cfg, nil
}
