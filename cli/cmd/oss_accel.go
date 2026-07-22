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

// M2 收尾阶段 O: a proper admin entry point for oss-accel's per-volume cold
// backend override (proto.OSSAccelBackendConfig, lcnode/oss_accel.go
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

	cmdOssAccelBackendSetUse   = "set [volname]"
	cmdOssAccelBackendSetShort = "set the volume's per-vol cold S3 backend override"
	cmdOssAccelBackendGetUse   = "get [volname]"
	cmdOssAccelBackendGetShort = "show the volume's per-vol cold S3 backend override, if any"
	cmdOssAccelBackendDelUse   = "delete [volname]"
	cmdOssAccelBackendDelShort = "remove the volume's per-vol override (falls back to the mover's global env config)"
	cmdOssAccelBackendShort    = "Manage a volume's per-vol cold S3 backend override"
)

func newOssAccelCmd(client *master.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:     cmdOssAccelUse,
		Short:   cmdOssAccelShort,
		Args:    cobra.MinimumNArgs(0),
		Aliases: []string{"ossaccel"},
	}
	proto.InitBufferPool(32768)
	cmd.AddCommand(newOssAccelBackendCmd(client), newOssAccelPinCmd(client))
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
		endpoint      string
		region        string
		bucket        string
		accessKeyEnv  string
		secretKeyEnv  string
		pathStyle     bool
		skipTLSVerify bool
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
			cfg := proto.OSSAccelBackendConfig{
				Endpoint:      endpoint,
				Region:        region,
				Bucket:        bucket,
				AccessKeyEnv:  accessKeyEnv,
				SecretKeyEnv:  secretKeyEnv,
				PathStyle:     pathStyle,
				SkipTLSVerify: skipTLSVerify,
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
			stdout("vol[%v] oss-accel backend override set:\n%v\n", volName, string(raw))
		},
	}
	cmd.Flags().StringVar(&endpoint, "endpoint", "", "S3-compatible endpoint (required)")
	cmd.Flags().StringVar(&region, "region", "", "S3 region (defaults to us-east-1 if empty)")
	cmd.Flags().StringVar(&bucket, "bucket", "", "bucket name (required)")
	cmd.Flags().StringVar(&accessKeyEnv, "access-key-env", "", "name of the lcnode env var holding the access key (defaults to the mover's global OSS_ACCEL_S3_AK if empty — credential VALUES are never stored here)")
	cmd.Flags().StringVar(&secretKeyEnv, "secret-key-env", "", "name of the lcnode env var holding the secret key (defaults to the mover's global OSS_ACCEL_S3_SK if empty)")
	cmd.Flags().BoolVar(&pathStyle, "path-style", false, "use S3 path-style addressing (MinIO/Ceph RGW)")
	cmd.Flags().BoolVar(&skipTLSVerify, "skip-tls-verify", false, "skip TLS certificate verification")
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
				stdout("vol[%v] has no oss-accel backend override — falls back to the mover's global env config\n", volName)
				return
			}
			var cfg proto.OSSAccelBackendConfig
			if err = json.Unmarshal(raw, &cfg); err != nil {
				stdout("vol[%v] oss-accel backend override is not valid JSON: %v\nraw: %v\n", volName, err, string(raw))
				return
			}
			stdout("vol[%v] oss-accel backend override:\n%v\n", volName, string(raw))
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
			stdout("vol[%v] oss-accel backend override removed — falls back to the mover's global env config\n", volName)
		},
	}
	return cmd
}
