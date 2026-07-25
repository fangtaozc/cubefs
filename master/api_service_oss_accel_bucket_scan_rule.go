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

package master

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
)

// Master HTTP admin surface for the oss-accel bucket-scan rule — one schedule
// per volume, mirrors api_service_oss_accel_changelog_rule.go exactly.
// Auth level matches the changelog/eviction rule routes ("plain", i.e. not
// wrapped in requireSyncAdminToken) — this is master's own rule CRUD
// surface, a different auth domain from lcnode's own HTTP endpoints.

const setOSSAccelBucketScanRuleBodyCap = 64 << 10

// setOSSAccelBucketScanRule handles POST /ossAccelBucketScanRule/set. Body is a
// proto.OSSAccelBucketScanRule; CreatedAt/UpdatedAt/LastRunAt/LastRunResult are
// server-controlled and reset here so a caller can't impersonate an
// existing record's run history.
func (m *Server) setOSSAccelBucketScanRule(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.OSSAccelBucketScanRuleSet))
	var err error
	defer func() { doStatAndMetric(proto.OSSAccelBucketScanRuleSet, metric, err, nil) }()

	body, rerr := io.ReadAll(io.LimitReader(r.Body, setOSSAccelBucketScanRuleBodyCap+1))
	if rerr != nil {
		err = rerr
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if len(body) > setOSSAccelBucketScanRuleBodyCap {
		err = fmt.Errorf("request body exceeds %d bytes", setOSSAccelBucketScanRuleBodyCap)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	rule := &proto.OSSAccelBucketScanRule{}
	if err = json.Unmarshal(body, rule); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if err = validateOSSAccelBucketScanRule(rule); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if _, err = m.cluster.getVol(rule.VolName); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeVolNotExists, Msg: err.Error()})
		return
	}

	existing := m.cluster.GetOSSAccelBucketScanRule(rule.VolName)
	now := time.Now()
	rule.UpdatedAt = now
	if existing != nil {
		rule.CreatedAt = existing.CreatedAt
		rule.LastRunAt = existing.LastRunAt
		rule.LastRunResult = existing.LastRunResult
	} else {
		rule.CreatedAt = now
	}

	if err = m.cluster.SetOSSAccelBucketScanRule(rule); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodePersistenceByRaft, Msg: err.Error()})
		return
	}
	AuditLog(r, "SetOSSAccelBucketScanRule", fmt.Sprintf("vol(%v) enabled(%v) intervalSeconds(%v) prefix(%v)",
		rule.VolName, rule.Enabled, rule.IntervalSeconds, rule.Prefix), nil)
	sendOkReply(w, r, newSuccessHTTPReply(rule))
}

// getOSSAccelBucketScanRule handles GET /ossAccelBucketScanRule/get?name=<vol>.
func (m *Server) getOSSAccelBucketScanRule(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.OSSAccelBucketScanRuleGet))
	var err error
	defer func() { doStatAndMetric(proto.OSSAccelBucketScanRuleGet, metric, err, nil) }()

	name, err := parseAndExtractName(r)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if _, err = m.cluster.getVol(name); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeVolNotExists, Msg: err.Error()})
		return
	}
	rule := m.cluster.GetOSSAccelBucketScanRule(name)
	if rule == nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: fmt.Sprintf("no oss-accel bucket-scan rule for vol %q", name)})
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(rule))
}

// deleteOSSAccelBucketScanRule handles POST /ossAccelBucketScanRule/delete?name=<vol>.
func (m *Server) deleteOSSAccelBucketScanRule(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.OSSAccelBucketScanRuleDelete))
	var err error
	defer func() { doStatAndMetric(proto.OSSAccelBucketScanRuleDelete, metric, err, nil) }()

	name, err := parseAndExtractName(r)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if _, err = m.cluster.getVol(name); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeVolNotExists, Msg: err.Error()})
		return
	}
	err = m.cluster.DeleteOSSAccelBucketScanRule(name)
	AuditLog(r, "DeleteOSSAccelBucketScanRule", fmt.Sprintf("vol(%v)", name), err)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
		return
	}
	msg := fmt.Sprintf("delete vol[%v] oss-accel bucket-scan rule successfully", name)
	log.LogWarn(msg)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

// validateOSSAccelBucketScanRule checks the fields a caller controls directly.
// Prefix is required — unlike audit/trash-purge's optional empty=whole-vol
// scope, a bucket scan has no whole-bucket mode by design: the operator must
// explicitly declare the scan boundary (see proto.OSSAccelBucketScanRule's
// doc comment for why an unscoped scan of a shared bucket is unsafe).
func validateOSSAccelBucketScanRule(rule *proto.OSSAccelBucketScanRule) error {
	if rule.VolName == "" {
		return fmt.Errorf("volName is required")
	}
	if rule.Prefix == "" {
		return fmt.Errorf("prefix is required — bucket scan has no whole-bucket mode, the operator must declare a scan boundary")
	}
	if rule.IntervalSeconds == 0 {
		return fmt.Errorf("intervalSeconds must be > 0")
	}
	return nil
}
