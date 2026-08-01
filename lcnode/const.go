// Copyright 2023 The CubeFS Authors.
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
	"regexp"

	"github.com/cubefs/cubefs/proto"
	"golang.org/x/time/rate"
)

const (
	configListen                     = proto.ListenPort
	configMasterAddr                 = proto.MasterAddr
	configSimpleQueueInitCapacityStr = "simpleQueueInitCapacity"
	configScanCheckIntervalStr       = "scanCheckInterval"
	configLcScanRoutineNumPerTaskStr = "lcScanRoutineNumPerTask"
	configLcScanLimitPerSecondStr    = "lcScanLimitPerSecond"
	// concurrent range-fetch worker count for oss-accel recall/prefetch
	// downloads (lcnode/oss_accel.go, GetConcurrent). See const default below
	// for why 16/64.
	configOssAccelRecallConcurrencyStr = "ossAccelRecallConcurrency"
	// 差距分析续三(聚合并发无上限): distinct from ossAccelRecallConcurrency
	// above — that one controls the download worker count INSIDE a single
	// recall (GetConcurrent's fan-out); this one caps how many DIFFERENT
	// files' recalls this lcnode process runs at once (across the manual
	// recall endpoint, eager prefetch sweeps, and batch prefetch tasks —
	// see oss_accel_recall_inflight_limit.go). Was unbounded before this;
	// ossAccelRecallLimit (util/concurrent.KeyConcurrentLimit) only
	// deduplicates concurrent attempts on the SAME inode, it was never a
	// total cap.
	configOssAccelMaxInflightRecallsStr = "ossAccelMaxInflightRecalls"
	configSnapshotRoutineNumPerTaskStr  = "snapshotRoutineNumPerTask"
	configLcNodeTaskCountLimit          = "lcNodeTaskCountLimit"
	configDelayDelMinute                = "delayDelMinute"
	configUseCreateTime                 = "useCreateTime"
	// 系统层面收尾: shared admin token gating the 7 oss-accel HTTP endpoints
	// (see oss_accel_auth.go). Empty = auth disabled.
	configLcnodeAdminToken = "lcnodeAdminToken"
)

// Default of configuration value
const (
	defaultListen                  = "80"
	ModuleName                     = "lcNode"
	defaultReadDirLimit            = 1000
	defaultScanCheckInterval       = 60
	defaultLcScanRoutineNumPerTask = 20
	maxLcScanRoutineNumPerTask     = 500
	// defaultOssAccelRecallConcurrency: recall/prefetch of a large (multi-GB)
	// object over a single S3 GET can't saturate a WAN link — this many
	// parallel range-fetch workers instead. 16 is a starting point, not a
	// measured optimum (no real-machine bandwidth data yet); maxOssAccel-
	// RecallConcurrency caps misconfiguration from starving the node's own
	// bandwidth/memory (each worker buffers ~PartSizeMiB).
	defaultOssAccelRecallConcurrency = 16
	maxOssAccelRecallConcurrency     = 64
	// defaultOssAccelMaxInflightRecalls: how many different files' recalls
	// (manual + eager prefetch + batch prefetch, combined) this lcnode
	// process runs at once, on top of the per-file worker fan-out above.
	// 128 is a starting point sized to "clearly higher than a single
	// file's own worker count so normal traffic never brushes this ceiling,
	// low enough to actually protect the node" — not a measured optimum
	// (no load-test data yet, same caveat as ossAccelRecallConcurrency's
	// default). maxOssAccelMaxInflightRecalls caps misconfiguration.
	defaultOssAccelMaxInflightRecalls = 128
	maxOssAccelMaxInflightRecalls     = 512
	defaultLcScanLimitPerSecond       = rate.Inf
	defaultLcScanLimitBurst           = 1000
	defaultUnboundedChanInitCapacity  = 10000
	defaultSimpleQueueInitCapacity    = 1000000
	defaultLcNodeTaskCountLimit       = 1
	maxLcNodeTaskCountLimit           = 20
	defaultDelayDelMinute             = 1440           // default retention min(1 day) of old eks after migration
	MaxSizePutOnce                    = int64(1) << 23 // 8MB
	DirTrashSkip                      = ".Trash"

	defaultAllocRetryInterval       = 100
	defaultWriteRetryInterval       = 100
	defaultExtenthandlerMaxRetryMin = 10
)

var (
	// Regular expression used to verify the configuration of the service listening port.
	// A valid service listening port configuration is a string containing only numbers.
	regexpListen               = regexp.MustCompile(`^(\d)+$`)
	simpleQueueInitCapacity    int
	scanCheckInterval          int64
	lcScanRoutineNumPerTask    int
	lcScanLimitPerSecond       rate.Limit
	ossAccelRecallConcurrency  int
	ossAccelMaxInflightRecalls int
	snapshotRoutineNumPerTask  int
	lcNodeTaskCountLimit       int
	maxDirChanNum              = 1000000
	delayDelMinute             uint64
	useCreateTime              bool
)
