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

package cmd

import (
	"encoding/json"
	"strings"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/spf13/cobra"
)

// M2 收尾阶段 O: a proper admin entry point for oss-accel's per-volume
// cold backend config (proto.OSSAccelBackendConfig, lcnode/oss_accel.go
// loadOssAccelS3ConfigFromVol), replacing the hand-rolled throwaway xattr
// tool used during M1/M2 verification. This is a VOLUME ROOT xattr, not a
// master-persisted object — there's no master HTTP endpoint for it (nor
// should there be one; objectnode's own per-vol policy/CORS config uses the
// exact same xattr-on-root-inode mechanism, see docs/plan/cubefs-oss-accel-m1-design.md
// DEC-M1 notes). So, like cli/cmd/quota.go's create/delete commands, this
// talks directly to a meta.MetaWrapper — never through master.

const (
	cmdOssAccelUse        = "oss-accel [COMMAND]"
	cmdOssAccelShort      = "Manage oss-accel (object-store accelerator) settings"
	cmdOssAccelBackendUse = "backend [COMMAND]"

	// "override" wording deliberately dropped: there is no deployment-wide
	// default bucket to override any more. This IS the volume's cold backend,
	// and a volume without one cannot use oss-accel at all.
	cmdOssAccelBackendSetUse   = "set [volname]"
	cmdOssAccelBackendSetShort = "set the volume's cold S3 backend (required before oss-accel can be used on it)"
	cmdOssAccelBackendGetUse   = "get [volname]"
	cmdOssAccelBackendGetShort = "show the volume's cold S3 backend"
	cmdOssAccelBackendDelUse   = "delete [volname]"
	cmdOssAccelBackendDelShort = "remove the volume's cold S3 backend (disables oss-accel for it)"
	cmdOssAccelBackendShort    = "Manage a volume's cold S3 backend"

	cmdOssAccelRoleUse = "role [COMMAND]"

	cmdOssAccelRoleSetUse   = "set [volname]"
	cmdOssAccelRoleSetShort = "set the volume's write role (primary, secondary, or readonly)"
	cmdOssAccelRoleGetUse   = "get [volname]"
	cmdOssAccelRoleGetShort = "show the volume's write role, if configured"
	cmdOssAccelRoleDelUse   = "delete [volname]"
	cmdOssAccelRoleDelShort = "remove the volume's role config (falls back to unrestricted primary)"
	cmdOssAccelRoleShort    = "Manage a volume's write role: who may mutate the shared S3 bucket (primary/secondary/readonly)"

	cmdOssAccelWTUse = "write-through [COMMAND]"

	cmdOssAccelWTSetUse   = "set [volname]"
	cmdOssAccelWTSetShort = "set the volume's write-through mode (off, async, or sync)"
	cmdOssAccelWTGetUse   = "get [volname]"
	cmdOssAccelWTGetShort = "show the volume's write-through mode, if configured"
	cmdOssAccelWTDelUse   = "delete [volname]"
	cmdOssAccelWTDelShort = "remove the volume's write-through config (falls back to off)"
	cmdOssAccelWTShort    = "Manage a volume's S3-gateway write-through mode: push newly PUT objects to the cold backend at write time (off/async/sync)"
)

func newOssAccelCmd(client *master.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:     cmdOssAccelUse,
		Short:   cmdOssAccelShort,
		Args:    cobra.MinimumNArgs(0),
		Aliases: []string{"ossaccel"},
	}
	proto.InitBufferPool(32768)
	cmd.AddCommand(newOssAccelBackendCmd(client), newOssAccelPinCmd(client), newOssAccelRoleCmd(client),
		newOssAccelWriteThroughCmd(client))
	return cmd
}

// M2/M3 收尾阶段 S: a way to actually SET the pin xattr the shared sweep
// walker (lcnode/oss_accel_walk.go) already respects — without this, pin
// would be a mechanism nothing could ever engage.
func newOssAccelPinCmd(client *master.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "pin [COMMAND]",
		Short: "Pin/unpin a file to exclude it from oss-accel background sweeps (TTL cleanup, coldest-first eviction)",
		Args:  cobra.MinimumNArgs(0),
	}
	cmd.AddCommand(newOssAccelPinSetCmd(client), newOssAccelPinUnsetCmd(client))
	return cmd
}

// ossAccelResolvePath walks path (leading "/" optional) from the volume
// root one segment at a time via Lookup_ll — mirrors
// lcnode/oss_accel.go's ensureOssAccelParentDir, but resolving the FULL
// path (including the leaf) since cfs-cli operates on an existing file, not
// a to-be-created one.
func ossAccelResolvePath(mw *meta.MetaWrapper, path string) (ino uint64, err error) {
	ino = proto.RootIno
	for _, seg := range strings.Split(strings.Trim(path, "/"), "/") {
		if seg == "" {
			continue
		}
		if ino, _, err = mw.Lookup_ll(ino, seg); err != nil {
			return 0, err
		}
	}
	return ino, nil
}

func newOssAccelPinSetCmd(client *master.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "set [volname] [path]",
		Short: "pin a file — excluded from TTL cleanup and coldest-first eviction",
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			setOssAccelPin(client, args[0], args[1], true)
		},
	}
	return cmd
}

func newOssAccelPinUnsetCmd(client *master.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "unset [volname] [path]",
		Short: "unpin a file — resumes participating in background sweeps",
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			setOssAccelPin(client, args[0], args[1], false)
		},
	}
	return cmd
}

func setOssAccelPin(client *master.MasterClient, volName, path string, pin bool) {
	mw, err := newOssAccelVolMetaWrapper(client, volName)
	if err != nil {
		stdout("NewMetaWrapper failed: %v\n", err)
		return
	}
	defer mw.Close()
	ino, err := ossAccelResolvePath(mw, path)
	if err != nil {
		stdout("resolve path %q failed: %v\n", path, err)
		return
	}
	if pin {
		if err = mw.XAttrSet_ll(ino, []byte(proto.XAttrKeyOSSAccelPin), []byte("true")); err != nil {
			stdout("pin failed: %v\n", err)
			return
		}
		stdout("vol[%v] path[%v] (ino %v) pinned — excluded from oss-accel background sweeps\n", volName, path, ino)
		return
	}
	if err = mw.XAttrDel_ll(ino, proto.XAttrKeyOSSAccelPin); err != nil {
		stdout("unpin failed: %v\n", err)
		return
	}
	stdout("vol[%v] path[%v] (ino %v) unpinned\n", volName, path, ino)
}

func newOssAccelBackendCmd(client *master.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   cmdOssAccelBackendUse,
		Short: cmdOssAccelBackendShort,
		Args:  cobra.MinimumNArgs(0),
	}
	cmd.AddCommand(
		newOssAccelBackendSetCmd(client),
		newOssAccelBackendGetCmd(client),
		newOssAccelBackendDeleteCmd(client),
	)
	return cmd
}

func newOssAccelVolMetaWrapper(client *master.MasterClient, volName string) (*meta.MetaWrapper, error) {
	return meta.NewMetaWrapper(&meta.MetaConfig{
		Volume:               volName,
		Masters:              client.Nodes(),
		DisableTrashByClient: true,
	})
}

func newOssAccelBackendSetCmd(client *master.MasterClient) *cobra.Command {
	var (
		endpoint                   string
		region                     string
		bucket                     string
		accessKeyEnv               string
		secretKeyEnv               string
		profile                    string
		pathStyle                  bool
		skipTLSVerify              bool
		allowBackendCredentialAuth bool
	)
	cmd := &cobra.Command{
		Use:   cmdOssAccelBackendSetUse,
		Short: cmdOssAccelBackendSetShort,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			volName := args[0]
			if endpoint == "" || bucket == "" {
				stdout("set oss-accel backend failed: --endpoint and --bucket are required\n")
				return
			}
			// 凭证来源必须显式声明。同样的规则在 sdk/ossaccel.LoadBackendConfig
			// 里也把关（那才是真正的闸门，xattr 可以被别的途径写入）；这里提前
			// 拦一次，是为了在敲命令时就报错，而不是等到第一次 flush。
			if profile == "" && (accessKeyEnv == "" || secretKeyEnv == "") {
				stdout("set oss-accel backend failed: no credential source declared.\n" +
					"  There is no implicit default credential — pick one:\n" +
					"    --profile <name>                             a section in the mounted shared-credentials file (recommended)\n" +
					"    --access-key-env <NAME> --secret-key-env <NAME>   both required together (discouraged: plaintext env vars)\n")
				return
			}
			cfg := proto.OSSAccelBackendConfig{
				Endpoint:                   endpoint,
				Region:                     region,
				Bucket:                     bucket,
				AccessKeyEnv:               accessKeyEnv,
				SecretKeyEnv:               secretKeyEnv,
				ProfileName:                profile,
				PathStyle:                  pathStyle,
				SkipTLSVerify:              skipTLSVerify,
				AllowBackendCredentialAuth: allowBackendCredentialAuth,
			}
			raw, err := json.Marshal(cfg)
			if err != nil {
				stdout("marshal oss-accel backend config failed: %v\n", err)
				return
			}
			mw, err := newOssAccelVolMetaWrapper(client, volName)
			if err != nil {
				stdout("NewMetaWrapper failed: %v\n", err)
				return
			}
			defer mw.Close()
			if err = mw.XAttrSet_ll(proto.RootIno, []byte(proto.XAttrKeyOSSAccelBackendConfig), raw); err != nil {
				stdout("set oss-accel backend config failed: %v\n", err)
				return
			}
			stdout("vol[%v] oss-accel cold backend set:\n%v\n", volName, string(raw))
		},
	}
	cmd.Flags().StringVar(&endpoint, "endpoint", "", "S3-compatible endpoint (required)")
	cmd.Flags().StringVar(&region, "region", "", "S3 region (defaults to us-east-1 if empty)")
	cmd.Flags().StringVar(&bucket, "bucket", "", "bucket name (required)")
	cmd.Flags().StringVar(&accessKeyEnv, "access-key-env", "", "name of the lcnode env var holding the access key. No longer defaults to OSS_ACCEL_S3_AK — must be given explicitly (together with --secret-key-env) when not using --profile. Credential VALUES are never stored here")
	cmd.Flags().StringVar(&secretKeyEnv, "secret-key-env", "", "name of the lcnode env var holding the secret key. See --access-key-env; both must be given together")
	cmd.Flags().StringVar(&profile, "profile", "", "named profile (a \"[name]\" section) in the mounted shared-credentials file to resolve this volume's ak/sk from. REQUIRED unless --access-key-env/--secret-key-env are given — there is no implicit default profile")
	cmd.Flags().BoolVar(&pathStyle, "path-style", false, "use S3 path-style addressing (MinIO/Ceph RGW)")
	cmd.Flags().BoolVar(&skipTLSVerify, "skip-tls-verify", false, "skip TLS certificate verification")
	cmd.Flags().BoolVar(&allowBackendCredentialAuth, "allow-backend-credential-auth", false,
		"allow S3 requests against ObjectNode signed with this backend's own AK/SK to authenticate as the volume owner (off by default)")
	return cmd
}

func newOssAccelBackendGetCmd(client *master.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   cmdOssAccelBackendGetUse,
		Short: cmdOssAccelBackendGetShort,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			volName := args[0]
			mw, err := newOssAccelVolMetaWrapper(client, volName)
			if err != nil {
				stdout("NewMetaWrapper failed: %v\n", err)
				return
			}
			defer mw.Close()
			info, err := mw.XAttrGet_ll(proto.RootIno, proto.XAttrKeyOSSAccelBackendConfig)
			if err != nil {
				stdout("get oss-accel backend config failed: %v\n", err)
				return
			}
			raw := info.Get(proto.XAttrKeyOSSAccelBackendConfig)
			if len(raw) == 0 {
				stdout("vol[%v] has no oss-accel cold backend configured — oss-accel is unusable on it until one is set "+
					"(there is no deployment-wide default bucket):\n"+
					"  cfs-cli oss-accel backend set %v --endpoint <ep> --bucket <bucket> [--region <r>]\n", volName, volName)
				return
			}
			var cfg proto.OSSAccelBackendConfig
			if err = json.Unmarshal(raw, &cfg); err != nil {
				stdout("vol[%v] oss-accel cold backend config is not valid JSON: %v\nraw: %v\n", volName, err, string(raw))
				return
			}
			stdout("vol[%v] oss-accel cold backend:\n%v\n", volName, string(raw))
		},
	}
	return cmd
}

func newOssAccelBackendDeleteCmd(client *master.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   cmdOssAccelBackendDelUse,
		Short: cmdOssAccelBackendDelShort,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			volName := args[0]
			mw, err := newOssAccelVolMetaWrapper(client, volName)
			if err != nil {
				stdout("NewMetaWrapper failed: %v\n", err)
				return
			}
			defer mw.Close()
			if err = mw.XAttrDel_ll(proto.RootIno, proto.XAttrKeyOSSAccelBackendConfig); err != nil {
				stdout("delete oss-accel backend config failed: %v\n", err)
				return
			}
			stdout("vol[%v] oss-accel cold backend removed — oss-accel is now unusable on this volume "+
				"(no deployment-wide default to fall back to); already-tiered cold files can no longer be recalled "+
				"until a backend is set again\n", volName)
		},
	}
	return cmd
}

// M4 第一纵切片: an admin entry point for oss-accel's per-volume write role
// (proto.OSSAccelRoleConfig, lcnode/oss_accel.go loadOssAccelRoleConfig) —
// same VOLUME ROOT xattr mechanism as newOssAccelBackendCmd above, mirrored
// structurally.
func newOssAccelRoleCmd(client *master.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   cmdOssAccelRoleUse,
		Short: cmdOssAccelRoleShort,
		Args:  cobra.MinimumNArgs(0),
	}
	cmd.AddCommand(
		newOssAccelRoleSetCmd(client),
		newOssAccelRoleGetCmd(client),
		newOssAccelRoleDeleteCmd(client),
	)
	return cmd
}

func newOssAccelRoleSetCmd(client *master.MasterClient) *cobra.Command {
	var (
		role          string
		ownedPrefixes []string
	)
	cmd := &cobra.Command{
		Use:   cmdOssAccelRoleSetUse,
		Short: cmdOssAccelRoleSetShort,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			volName := args[0]
			if !proto.IsValidOSSAccelRole(role) {
				stdout("set oss-accel role failed: --role must be %q, %q, or %q\n",
					proto.OSSAccelRolePrimary, proto.OSSAccelRoleSecondary, proto.OSSAccelRoleReadOnly)
				return
			}
			// 形态收敛: canonicalize before storing, so `role get` shows what is
			// actually enforced rather than something lcnode silently rewrites on
			// read. Prefixes match S3 KEYS, which have no leading slash — a stored
			// "/ckpt" used to match nothing at all and quietly block every write.
			canonical := canonicalOssAccelOwnedPrefixes(ownedPrefixes)
			if len(ownedPrefixes) > 0 && !sameStringSlice(canonical, ownedPrefixes) {
				stdout("note: --owned-prefix normalized %v -> %v (S3 keys have no leading slash)\n", ownedPrefixes, canonical)
			}
			if len(canonical) > 0 && role != proto.OSSAccelRoleSecondary {
				stdout("warning: --owned-prefix is ignored for role=%v (only role=%v consults it); storing it anyway so a later switch to secondary can reuse it\n",
					role, proto.OSSAccelRoleSecondary)
			}
			cfg := proto.OSSAccelRoleConfig{Role: role, OwnedPrefixes: canonical}
			raw, err := json.Marshal(cfg)
			if err != nil {
				stdout("marshal oss-accel role config failed: %v\n", err)
				return
			}
			mw, err := newOssAccelVolMetaWrapper(client, volName)
			if err != nil {
				stdout("NewMetaWrapper failed: %v\n", err)
				return
			}
			defer mw.Close()
			if err = mw.XAttrSet_ll(proto.RootIno, []byte(proto.XAttrKeyOSSAccelRoleConfig), raw); err != nil {
				stdout("set oss-accel role config failed: %v\n", err)
				return
			}
			stdout("vol[%v] oss-accel role set:\n%v\n", volName, string(raw))
		},
	}
	cmd.Flags().StringVar(&role, "role", "", "primary, secondary, or readonly (required). primary: unrestricted. secondary: another CubeFS cluster owns the bucket's other prefixes; writes allowed only under --owned-prefix. readonly: the bucket belongs to an external system — all writes and all destructive housekeeping (orphan quarantine, rename-drift relocate, trash purge) are refused, detection still runs and reports")
	cmd.Flags().StringSliceVar(&ownedPrefixes, "owned-prefix", nil, "S3 KEY prefix this cluster may still write despite role=secondary (repeatable). No leading slash — write \"ckpt/\", not \"/ckpt\". Matched at path-segment boundaries, so \"ckpt\" covers \"ckpt/...\" but not \"ckptx/...\". Ignored for role=primary and role=readonly")
	return cmd
}

// canonicalOssAccelOwnedPrefixes mirrors lcnode's normalizeOssAccelOwnedPrefixes
// (lcnode/oss_accel_role.go) — kept as a small local copy rather than exported
// from lcnode, since cli importing lcnode for four lines of string handling
// would be a much worse dependency than the duplication. Both sides normalize,
// so a mismatch degrades to "stored form is uglier than enforced form", never
// to a difference in what is enforced.
func canonicalOssAccelOwnedPrefixes(prefixes []string) []string {
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

func sameStringSlice(a, b []string) bool {
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

func newOssAccelRoleGetCmd(client *master.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   cmdOssAccelRoleGetUse,
		Short: cmdOssAccelRoleGetShort,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			volName := args[0]
			mw, err := newOssAccelVolMetaWrapper(client, volName)
			if err != nil {
				stdout("NewMetaWrapper failed: %v\n", err)
				return
			}
			defer mw.Close()
			info, err := mw.XAttrGet_ll(proto.RootIno, proto.XAttrKeyOSSAccelRoleConfig)
			if err != nil {
				stdout("get oss-accel role config failed: %v\n", err)
				return
			}
			raw := info.Get(proto.XAttrKeyOSSAccelRoleConfig)
			if len(raw) == 0 {
				stdout("vol[%v] has no oss-accel role config — defaults to unrestricted %q\n", volName, proto.OSSAccelRolePrimary)
				return
			}
			var cfg proto.OSSAccelRoleConfig
			if err = json.Unmarshal(raw, &cfg); err != nil {
				stdout("vol[%v] oss-accel role config is not valid JSON: %v\nraw: %v\n", volName, err, string(raw))
				return
			}
			stdout("vol[%v] oss-accel role:\n%v\n", volName, string(raw))
		},
	}
	return cmd
}

func newOssAccelRoleDeleteCmd(client *master.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   cmdOssAccelRoleDelUse,
		Short: cmdOssAccelRoleDelShort,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			volName := args[0]
			mw, err := newOssAccelVolMetaWrapper(client, volName)
			if err != nil {
				stdout("NewMetaWrapper failed: %v\n", err)
				return
			}
			defer mw.Close()
			if err = mw.XAttrDel_ll(proto.RootIno, proto.XAttrKeyOSSAccelRoleConfig); err != nil {
				stdout("delete oss-accel role config failed: %v\n", err)
				return
			}
			stdout("vol[%v] oss-accel role config removed — falls back to unrestricted %q\n", volName, proto.OSSAccelRolePrimary)
		},
	}
	return cmd
}

// 差距分析续(同步预热): admin entry point for the per-volume write-through mode
// (proto.OSSAccelWriteThroughConfig) — same VOLUME ROOT xattr mechanism as
// newOssAccelBackendCmd/newOssAccelRoleCmd above, mirrored structurally.
//
// Kept as a separate xattr from the backend config on purpose; see
// proto.XAttrKeyOSSAccelWriteThrough's doc comment for why folding it into
// oss-accel.backend would break env-configured volumes.
func newOssAccelWriteThroughCmd(client *master.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:     cmdOssAccelWTUse,
		Short:   cmdOssAccelWTShort,
		Args:    cobra.MinimumNArgs(0),
		Aliases: []string{"writethrough", "wt"},
	}
	cmd.AddCommand(
		newOssAccelWriteThroughSetCmd(client),
		newOssAccelWriteThroughGetCmd(client),
		newOssAccelWriteThroughDeleteCmd(client),
	)
	return cmd
}

func newOssAccelWriteThroughSetCmd(client *master.MasterClient) *cobra.Command {
	var mode string
	cmd := &cobra.Command{
		Use:   cmdOssAccelWTSetUse,
		Short: cmdOssAccelWTSetShort,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			volName := args[0]
			if !proto.IsValidOSSAccelWriteThroughMode(mode) {
				stdout("set oss-accel write-through failed: --mode must be %q, %q, or %q\n",
					proto.OSSAccelWriteThroughOff, proto.OSSAccelWriteThroughAsync, proto.OSSAccelWriteThroughSync)
				return
			}
			cfg := proto.OSSAccelWriteThroughConfig{Mode: mode}
			raw, err := json.Marshal(cfg)
			if err != nil {
				stdout("marshal oss-accel write-through config failed: %v\n", err)
				return
			}
			mw, err := newOssAccelVolMetaWrapper(client, volName)
			if err != nil {
				stdout("NewMetaWrapper failed: %v\n", err)
				return
			}
			defer mw.Close()
			if err = mw.XAttrSet_ll(proto.RootIno, []byte(proto.XAttrKeyOSSAccelWriteThrough), raw); err != nil {
				stdout("set oss-accel write-through config failed: %v\n", err)
				return
			}
			stdout("vol[%v] oss-accel write-through set:\n%v\n", volName, string(raw))
			if mode == proto.OSSAccelWriteThroughSync {
				stdout("note: sync mode makes a failed cold-backend upload fail the S3 PUT, and adds the upload to PUT latency\n")
			}
			if mode != proto.OSSAccelWriteThroughOff {
				stdout("note: applies to S3-gateway (ObjectNode) writes only — POSIX writes still reach the cold tier via the flush policy\n")
			}
		},
	}
	cmd.Flags().StringVar(&mode, "mode", "", "off, async, or sync (required). off: cold backend written only by the flush policy. async: ack the PUT first, flush in the background (PUT latency unchanged). sync: ack only after the cold-backend upload succeeds; a failed upload fails the PUT")
	return cmd
}

func newOssAccelWriteThroughGetCmd(client *master.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   cmdOssAccelWTGetUse,
		Short: cmdOssAccelWTGetShort,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			volName := args[0]
			mw, err := newOssAccelVolMetaWrapper(client, volName)
			if err != nil {
				stdout("NewMetaWrapper failed: %v\n", err)
				return
			}
			defer mw.Close()
			info, err := mw.XAttrGet_ll(proto.RootIno, proto.XAttrKeyOSSAccelWriteThrough)
			if err != nil {
				stdout("get oss-accel write-through config failed: %v\n", err)
				return
			}
			raw := info.Get(proto.XAttrKeyOSSAccelWriteThrough)
			if len(raw) == 0 {
				stdout("vol[%v] has no oss-accel write-through config — defaults to %q\n", volName, proto.OSSAccelWriteThroughOff)
				return
			}
			var cfg proto.OSSAccelWriteThroughConfig
			if err = json.Unmarshal(raw, &cfg); err != nil {
				stdout("vol[%v] oss-accel write-through config is not valid JSON: %v\nraw: %v\n", volName, err, string(raw))
				return
			}
			stdout("vol[%v] oss-accel write-through:\n%v\n", volName, string(raw))
		},
	}
	return cmd
}

func newOssAccelWriteThroughDeleteCmd(client *master.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   cmdOssAccelWTDelUse,
		Short: cmdOssAccelWTDelShort,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			volName := args[0]
			mw, err := newOssAccelVolMetaWrapper(client, volName)
			if err != nil {
				stdout("NewMetaWrapper failed: %v\n", err)
				return
			}
			defer mw.Close()
			if err = mw.XAttrDel_ll(proto.RootIno, proto.XAttrKeyOSSAccelWriteThrough); err != nil {
				stdout("delete oss-accel write-through config failed: %v\n", err)
				return
			}
			stdout("vol[%v] oss-accel write-through config removed — falls back to %q\n", volName, proto.OSSAccelWriteThroughOff)
		},
	}
	return cmd
}
