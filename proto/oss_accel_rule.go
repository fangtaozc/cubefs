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

import "time"

// M2 production automation: master-persisted schedule for lcnode's
// changelog sync (lcnode/oss_accel.go httpServiceOssAccelChangelogSync /
// runOssAccelChangelogSync), replacing the manual-curl-only trigger from
// the first vertical slice. See docs/plan/cubefs-oss-accel-m2-design.md.
//
// One rule per volume — mirrors LcConfiguration's per-vol granularity
// (proto/lifecycle.go), not SyncRule's independent-ID model: a volume
// either has a changelog sync schedule or it doesn't, so there's no
// separate "list of rule IDs" concept to manage.

// OSSAccelChangelogRule is the master-persisted, per-volume changelog sync
// schedule. IntervalSeconds is a plain polling period, not a cron
// expression — changelog sync is a continuous catch-up poll, not a
// calendar-scheduled job, so cron's date/time semantics buy nothing here.
type OSSAccelChangelogRule struct {
	VolName         string    `json:"volName"`
	Prefix          string    `json:"prefix,omitempty"`
	ChangelogKey    string    `json:"changelogKey,omitempty"`
	IntervalSeconds uint32    `json:"intervalSeconds"`
	Enabled         bool      `json:"enabled"`
	CreatedAt       time.Time `json:"createdAt"`
	UpdatedAt       time.Time `json:"updatedAt"`
	LastRunAt       time.Time `json:"lastRunAt,omitempty"`
	LastRunResult   string    `json:"lastRunResult,omitempty"`

	// SkipAfterFailures: once the SAME stuck-at-cursor changelog line has
	// failed this many consecutive sync runs, lcnode logs it and advances
	// the cursor past it rather than blocking the volume's sync
	// indefinitely (design doc §5's documented limitation). 0 (default)
	// preserves the original behavior — never skip, wait for a manual fix.
	SkipAfterFailures uint32 `json:"skipAfterFailures,omitempty"`
	// ConsecutiveFailures is server-maintained: incremented whenever a run
	// reports Failed>0, reset to 0 on a clean run. Not caller-settable via
	// /set (silently preserved from the existing record, like LastRunAt).
	ConsecutiveFailures uint32 `json:"consecutiveFailures,omitempty"`

	// PlaceholderTTLSeconds: reclaim a materialized-but-never-read
	// placeholder inode (proto.ColdStateMaterialized) once it's been sitting
	// unread this long. 0 (default) disables reclamation entirely — a
	// materialized placeholder lives forever until the FIRST successful
	// recall, same as before this field existed.
	PlaceholderTTLSeconds uint32 `json:"placeholderTTLSeconds,omitempty"`
}

// OSSAccelChangelogSyncTaskRequest is the AdminTask payload master sends to
// a live lcnode (dispatched via Cluster.addLcNodeTasks, the same primitive
// lifecycle scanning uses — NOT a new HTTP call path). Deliberately NOT
// proto.RuleTask/proto.Rule — those model S3 lifecycle expiration policies,
// a completely different shape.
type OSSAccelChangelogSyncTaskRequest struct {
	MasterAddr            string
	LcNodeAddr            string
	VolName               string
	Prefix                string
	ChangelogKey          string
	SkipAfterFailures     uint32
	ConsecutiveFailures   uint32
	PlaceholderTTLSeconds uint32
}

// OSSAccelChangelogSyncTaskResponse is what lcnode reports back after
// running the task (mirrors LcNodeRuleTaskResponse's ack shape,
// proto/lifecycle.go).
type OSSAccelChangelogSyncTaskResponse struct {
	VolName   string
	LcNode    string
	StartTime *time.Time
	EndTime   *time.Time
	Done      bool
	StartErr  string
	Processed int
	Skipped   int
	Failed    int
	Cursor    uint64
	Swept     int
}
