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

package lcnode

import (
	"errors"
	"testing"

	"github.com/cubefs/cubefs/proto"
)

// The permissive default is the single most important property in this file:
// nearly every volume in existence has no role xattr, and any change that
// makes them non-permissive is a cluster-wide write outage.
func TestParseOssAccelRoleConfigEmptyIsPermissive(t *testing.T) {
	cfg, err := parseOssAccelRoleConfig("")
	if err != nil {
		t.Fatalf("empty raw must not error, got %v", err)
	}
	if cfg.Role != proto.OSSAccelRolePrimary {
		t.Fatalf("empty raw must default to primary, got %q", cfg.Role)
	}
	if !ossAccelBucketWriteAllowed(cfg) {
		t.Fatal("unconfigured volume must be allowed to write the bucket")
	}
	if !ossAccelKeyWriteAllowed(cfg, "anything/at/all") {
		t.Fatal("unconfigured volume must be allowed to write any key")
	}
	if ossAccelBucketExternallyOwned(cfg) {
		t.Fatal("unconfigured volume must not be treated as externally owned")
	}
}

func TestParseOssAccelRoleConfigValidRoles(t *testing.T) {
	for _, role := range []string{proto.OSSAccelRolePrimary, proto.OSSAccelRoleSecondary, proto.OSSAccelRoleReadOnly} {
		cfg, err := parseOssAccelRoleConfig(`{"role":"` + role + `"}`)
		if err != nil {
			t.Fatalf("role %q must parse, got %v", role, err)
		}
		if cfg.Role != role {
			t.Fatalf("role %q round-trip got %q", role, cfg.Role)
		}
	}
}

func TestParseOssAccelRoleConfigRejects(t *testing.T) {
	cases := map[string]string{
		"unknown role":      `{"role":"bogus"}`,
		"missing role key":  `{"ownedPrefixes":["ckpt/"]}`,
		"malformed json":    `{"role":`,
		"json null role":    `{"role":null}`,
		"not an object":     `"primary"`,
		"empty json object": `{}`,
	}
	for name, raw := range cases {
		if _, err := parseOssAccelRoleConfig(raw); err == nil {
			t.Fatalf("%s: expected rejection, got nil error (a config nobody can read must not resolve to the permissive default)", name)
		}
	}
}

// Fix B matrix. The "/ckpt" row is the bug: a leading slash made the prefix
// match nothing at all, silently blocking every write on the volume.
func TestNormalizeOssAccelOwnedPrefixes(t *testing.T) {
	cases := []struct {
		name string
		in   []string
		want []string
	}{
		{"already canonical", []string{"ckpt/"}, []string{"ckpt/"}},
		{"leading slash stripped", []string{"/ckpt"}, []string{"ckpt"}},
		{"leading slash with trailing", []string{"/ckpt/"}, []string{"ckpt/"}},
		{"whitespace trimmed", []string{"  ckpt/  "}, []string{"ckpt/"}},
		{"empty dropped", []string{"", "ckpt/"}, []string{"ckpt/"}},
		{"bare slash dropped", []string{"/"}, nil},
		{"dedupe after normalize", []string{"/ckpt", "ckpt"}, []string{"ckpt"}},
		{"order preserved", []string{"b/", "a/"}, []string{"b/", "a/"}},
		{"nil in nil out", nil, nil},
		{"all empty yields nil", []string{"", "  ", "/"}, nil},
	}
	for _, c := range cases {
		got := normalizeOssAccelOwnedPrefixes(c.in)
		if !equalStringSlices(got, c.want) {
			t.Errorf("%s: normalize(%v) = %v, want %v", c.name, c.in, got, c.want)
		}
	}
}

// Segment awareness: a secondary delegated "ckpt" must not thereby gain write
// authority over "ckptx/".
func TestOssAccelKeyUnderPrefix(t *testing.T) {
	cases := []struct {
		key, prefix string
		want        bool
	}{
		{"ckpt", "ckpt", true},
		{"ckpt/model.bin", "ckpt", true},
		{"ckpt/deep/model.bin", "ckpt", true},
		{"ckptx/model.bin", "ckpt", false},
		{"ckptx", "ckpt", false},
		{"ckpt/model.bin", "ckpt/", true},
		{"ckptx/model.bin", "ckpt/", false},
		{"ckpt", "ckpt/", false}, // trailing-slash prefix requires the slash
		{"other/model.bin", "ckpt", false},
		{"anything", "", false}, // empty prefix grants nothing
	}
	for _, c := range cases {
		if got := ossAccelKeyUnderPrefix(c.key, c.prefix); got != c.want {
			t.Errorf("ossAccelKeyUnderPrefix(%q, %q) = %v, want %v", c.key, c.prefix, got, c.want)
		}
	}
}

// The three predicates across the three roles. The readonly-vs-secondary
// difference in the externallyOwned column is the whole reason there are three
// predicates instead of two — collapsing them is the most likely refactor slip.
func TestOssAccelPredicateMatrix(t *testing.T) {
	primary := &proto.OSSAccelRoleConfig{Role: proto.OSSAccelRolePrimary}
	secondaryNone := &proto.OSSAccelRoleConfig{Role: proto.OSSAccelRoleSecondary}
	secondaryOwned := &proto.OSSAccelRoleConfig{Role: proto.OSSAccelRoleSecondary, OwnedPrefixes: []string{"ckpt/"}}
	readonly := &proto.OSSAccelRoleConfig{Role: proto.OSSAccelRoleReadOnly}
	readonlyOwned := &proto.OSSAccelRoleConfig{Role: proto.OSSAccelRoleReadOnly, OwnedPrefixes: []string{"ckpt/"}}

	cases := []struct {
		name                          string
		cfg                           *proto.OSSAccelRoleConfig
		bucketWrite, externallyOwned  bool
		keyInPrefix, keyOutsidePrefix bool
	}{
		{"primary", primary, true, false, true, true},
		{"secondary no prefixes", secondaryNone, true, false, false, false},
		{"secondary owning ckpt/", secondaryOwned, true, false, true, false},
		{"readonly", readonly, false, true, false, false},
		// OwnedPrefixes must be ignored for readonly — a stale prefix list left
		// over from a previous secondary config must not punch a hole in it.
		{"readonly with stale prefixes", readonlyOwned, false, true, false, false},
	}
	for _, c := range cases {
		if got := ossAccelBucketWriteAllowed(c.cfg); got != c.bucketWrite {
			t.Errorf("%s: bucketWriteAllowed = %v, want %v", c.name, got, c.bucketWrite)
		}
		if got := ossAccelBucketExternallyOwned(c.cfg); got != c.externallyOwned {
			t.Errorf("%s: bucketExternallyOwned = %v, want %v", c.name, got, c.externallyOwned)
		}
		if got := ossAccelKeyWriteAllowed(c.cfg, "ckpt/model.bin"); got != c.keyInPrefix {
			t.Errorf("%s: keyWriteAllowed(in-prefix) = %v, want %v", c.name, got, c.keyInPrefix)
		}
		if got := ossAccelKeyWriteAllowed(c.cfg, "datasets/train.bin"); got != c.keyOutsidePrefix {
			t.Errorf("%s: keyWriteAllowed(outside-prefix) = %v, want %v", c.name, got, c.keyOutsidePrefix)
		}
	}
}

// A secondary is NOT externally owned: it shares a collectively-owned bucket,
// so a dangling reference or checksum mismatch there is a genuine fault worth
// marking even for prefixes this cluster may not write.
func TestSecondaryIsNotExternallyOwned(t *testing.T) {
	cfg := &proto.OSSAccelRoleConfig{Role: proto.OSSAccelRoleSecondary}
	if ossAccelBucketWriteAllowed(cfg) != true {
		t.Fatal("secondary must retain bucket-level write permission (for the shared changelog object)")
	}
	if ossAccelBucketExternallyOwned(cfg) {
		t.Fatal("secondary must NOT be externally owned — it must keep marking ColdStateError")
	}
}

// nil cfg is permissive-with-a-warning by deliberate choice: a silent
// cluster-wide write block would be a worse failure than a loud slip.
func TestNilRoleConfigIsPermissive(t *testing.T) {
	if !ossAccelBucketWriteAllowed(nil) {
		t.Fatal("nil cfg must be permissive")
	}
	if !ossAccelKeyWriteAllowed(nil, "any/key") {
		t.Fatal("nil cfg must be permissive for any key")
	}
	if ossAccelBucketExternallyOwned(nil) {
		t.Fatal("nil cfg must not be treated as externally owned")
	}
}

func TestOssAccelWriteForbiddenErrfWraps(t *testing.T) {
	err := ossAccelWriteForbiddenErrf(&proto.OSSAccelRoleConfig{Role: proto.OSSAccelRoleReadOnly}, "v", "flush", "k")
	if !errors.Is(err, errOssAccelWriteForbidden) {
		t.Fatal("refusal error must wrap errOssAccelWriteForbidden so handlers can map it to 403")
	}
	if err := ossAccelWriteForbiddenErrf(nil, "v", "flush", "k"); !errors.Is(err, errOssAccelWriteForbidden) {
		t.Fatal("nil cfg must still produce a wrapped refusal, not panic")
	}
}

// Normalization must apply on the read path too, so a volume whose xattr was
// written before the CLI canonicalized prefixes still enforces correctly.
func TestParseNormalizesStoredPrefixes(t *testing.T) {
	cfg, err := parseOssAccelRoleConfig(`{"role":"secondary","ownedPrefixes":["/ckpt"]}`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if !ossAccelKeyWriteAllowed(cfg, "ckpt/model.bin") {
		t.Fatal(`stored "/ckpt" must match key "ckpt/model.bin" after read-path normalization (this is the Fix B bug)`)
	}
	if ossAccelKeyWriteAllowed(cfg, "ckptx/model.bin") {
		t.Fatal(`stored "/ckpt" must NOT match "ckptx/model.bin" (segment awareness)`)
	}
}
