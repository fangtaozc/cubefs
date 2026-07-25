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

// 形态收敛: oss-accel 的写权限门禁。原先只有一个谓词(ossAccelWriteAllowed)
// 且只挂在 flush 一个调用点上,而 oss-accel 一共有 5 条会改动外部 S3 桶的
// 路径(flush Put / changelog Put / relocate Rename / 孤儿隔离 Rename /
// trash purge Delete)——一个本意"只读消费"的卷仍然能改名、能隔离、能永久
// 删除别人桶里的对象。这个文件把门禁收成三个语义清晰的谓词,供全部 5 条
// 路径统一调用。
//
// 为什么是三个而不是一个:
//   - Bucket 级和 Key 级必须分开,因为共享的 changelog 对象按构造就落在
//     任何 OwnedPrefixes 之外(它在桶根)。用 Key 谓词去管它会把一个合法
//     写自己前缀的 secondary 的 M4 跨集群传播悄悄杀掉。
//   - "桶是不是外部的"和"我能不能写"今天数值上互补,但回答的是不同问题,
//     且驱动不同行为(前者决定要不要写 sticky 的 ColdStateError 标记)。
//     分开命名是为了防止后人把它们合并掉。
package lcnode

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

// errOssAccelWriteForbidden is the sentinel HTTP handlers match with
// errors.As/errors.Is to map a role refusal to 403 instead of a generic 500.
// Without it a refusal is indistinguishable from a real S3 failure at the
// API boundary — and "was this refused, or did it just fail?" is exactly the
// question the whole gate exists to answer.
var errOssAccelWriteForbidden = errors.New("oss-accel: volume write role forbids this S3 mutation")

// defaultOssAccelRoleConfig is the fully permissive config used for any
// volume without a role xattr — i.e. nearly every volume. This must stay
// unrestricted: the gate is opt-in, never opt-out.
func defaultOssAccelRoleConfig() *proto.OSSAccelRoleConfig {
	return &proto.OSSAccelRoleConfig{Role: proto.OSSAccelRolePrimary}
}

// parseOssAccelRoleConfig turns the raw xattr value into a validated,
// normalized config. Empty raw (xattr unset) is the permissive default, not
// an error. A present-but-malformed value IS an error — callers must not fall
// back silently, since a config nobody can parse is not a config anyone
// should be relied on to have honored.
//
// Split out from loadOssAccelRoleConfig purely so the parse/normalize/reject
// behavior is unit-testable without a live metanode.
func parseOssAccelRoleConfig(raw string) (*proto.OSSAccelRoleConfig, error) {
	if raw == "" {
		return defaultOssAccelRoleConfig(), nil
	}
	var cfg proto.OSSAccelRoleConfig
	if jerr := json.Unmarshal([]byte(raw), &cfg); jerr != nil {
		return nil, fmt.Errorf("oss-accel per-vol role config on root inode is not valid JSON: %v", jerr)
	}
	// A JSON object with ownedPrefixes but no "role" key unmarshals to
	// Role=="" and is rejected here. That's intended: an unreadable intent
	// must not silently resolve to the permissive default.
	if !proto.IsValidOSSAccelRole(cfg.Role) {
		return nil, fmt.Errorf("oss-accel per-vol role config has invalid role %q (want %q, %q, or %q)",
			cfg.Role, proto.OSSAccelRolePrimary, proto.OSSAccelRoleSecondary, proto.OSSAccelRoleReadOnly)
	}
	normalized := normalizeOssAccelOwnedPrefixes(cfg.OwnedPrefixes)
	if len(normalized) != len(cfg.OwnedPrefixes) || !equalStringSlices(normalized, cfg.OwnedPrefixes) {
		// Only fires for role-configured volumes, so this is not log spam —
		// and it's worth being loud: a leading-slash prefix used to match
		// nothing at all, silently blocking every write on the volume.
		log.LogWarnf("parseOssAccelRoleConfig: ownedPrefixes normalized %v -> %v (prefixes match S3 keys, which have no leading slash; re-run `cfs-cli oss-accel role set` to store the canonical form)",
			cfg.OwnedPrefixes, normalized)
	}
	cfg.OwnedPrefixes = normalized
	return &cfg, nil
}

// normalizeOssAccelOwnedPrefixes canonicalizes stored prefixes: trim spaces,
// strip the leading slash (S3 keys never have one — see normalizeOssAccelKey),
// drop empties, dedupe while preserving order. Applied on both the read path
// (here) and the CLI write path, so old non-canonical values keep working and
// newly written ones are already canonical.
func normalizeOssAccelOwnedPrefixes(prefixes []string) []string {
	if len(prefixes) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(prefixes))
	out := make([]string, 0, len(prefixes))
	for _, p := range prefixes {
		p = strings.TrimPrefix(strings.TrimSpace(p), "/")
		if p == "" {
			continue
		}
		if _, dup := seen[p]; dup {
			continue
		}
		seen[p] = struct{}{}
		out = append(out, p)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// ossAccelKeyUnderPrefix reports whether s3key falls under prefix, matching at
// PATH SEGMENT boundaries: prefix "ckpt" covers the key "ckpt" and everything
// under "ckpt/", but NOT "ckptx/model.bin". A bare strings.HasPrefix would
// have granted a secondary delegated "ckpt" write authority over "ckptx/" too.
//
// Deliberately NOT shared with the HasPrefix calls used for scan scoping
// (audit/integrity/prefetch/register prefix filters): those answer "is this in
// the range I was asked to look at", a different question from "do I have
// authority to mutate this". Merging them would couple write authority to
// scan ergonomics.
func ossAccelKeyUnderPrefix(s3key, prefix string) bool {
	if prefix == "" {
		return false
	}
	if s3key == prefix {
		return true
	}
	if strings.HasSuffix(prefix, "/") {
		return strings.HasPrefix(s3key, prefix)
	}
	return strings.HasPrefix(s3key, prefix+"/")
}

// ossAccelBucketWriteAllowed reports whether this volume may write to the
// shared bucket AT ALL, ignoring which key. Use for bucket-infrastructure
// objects that sit outside any OwnedPrefixes by construction — today that's
// the shared changelog object at the bucket root. Also the right pre-flight
// check for a sweep that would otherwise attempt many per-key writes it can
// never be allowed to make.
//
// nil cfg is treated as the permissive default (with a warning) rather than
// fail-closed: "no role xattr means fully permissive" is a hard requirement,
// and a loud-but-working slip in some future call site is far better than a
// silent cluster-wide write block.
func ossAccelBucketWriteAllowed(cfg *proto.OSSAccelRoleConfig) bool {
	if cfg == nil {
		log.LogWarnf("ossAccelBucketWriteAllowed: nil role config — treating as unrestricted primary; caller should load the config")
		return true
	}
	// Switch on the known-restrictive values rather than testing != primary,
	// so any value that passed IsValidOSSAccelRole defaults to permissive.
	switch cfg.Role {
	case proto.OSSAccelRoleReadOnly:
		return false
	default:
		return true
	}
}

// ossAccelKeyWriteAllowed reports whether this volume may mutate this specific
// S3 key. Bucket-level permission is the base case; a secondary additionally
// requires the key to fall under one of its delegated OwnedPrefixes.
//
// Use for every per-object mutation: flush Put, relocate Rename (check BOTH
// the old and new key — a relocate deletes one object and creates another),
// orphan-quarantine Rename, and trash-purge Delete (check the ORIGINAL key
// the trashed object came from, not the .trash/ path).
func ossAccelKeyWriteAllowed(cfg *proto.OSSAccelRoleConfig, s3key string) bool {
	if !ossAccelBucketWriteAllowed(cfg) {
		return false
	}
	if cfg == nil || cfg.Role != proto.OSSAccelRoleSecondary {
		return true
	}
	for _, p := range cfg.OwnedPrefixes {
		if ossAccelKeyUnderPrefix(s3key, p) {
			return true
		}
	}
	return false
}

// ossAccelBucketExternallyOwned reports whether the backing bucket belongs to
// a system outside CubeFS entirely. Used to suppress writing the sticky
// ColdStateError mark (audit's dangling-reference direction and integrity's
// checksum mismatch): against an externally-written bucket a mismatch or a
// missing key most likely means the owner legitimately changed the object,
// and ColdStateError has no clearing path for a committed-cold file.
//
// Numerically the complement of ossAccelBucketWriteAllowed today, but kept
// separate because it answers a different question. A secondary shares a
// COLLECTIVELY-OWNED bucket, so a missing object there is a genuine fault
// worth marking even though this cluster may not write that prefix — merging
// these two predicates would silently lose that distinction.
func ossAccelBucketExternallyOwned(cfg *proto.OSSAccelRoleConfig) bool {
	if cfg == nil {
		return false
	}
	return cfg.Role == proto.OSSAccelRoleReadOnly
}

// ossAccelWriteForbiddenErrf builds a refusal error wrapping
// errOssAccelWriteForbidden, so an HTTP handler can errors.Is it to 403 while
// the message still names the role, operation and key for the operator.
func ossAccelWriteForbiddenErrf(cfg *proto.OSSAccelRoleConfig, vol, op, s3key string) error {
	role := "<nil>"
	if cfg != nil {
		role = cfg.Role
	}
	return fmt.Errorf("%w: vol(%v) role(%v) op(%v) s3key(%v)", errOssAccelWriteForbidden, vol, role, op, s3key)
}
