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

// Master HTTP admin surface for the oss-accel integrity rule — one schedule
// per volume, mirrors api_service_oss_accel_changelog_rule.go exactly.
// Auth level matches the changelog/eviction rule routes ("plain", i.e. not
// wrapped in requireSyncAdminToken) — this is master's own rule CRUD
// surface, a different auth domain from lcnode's own HTTP endpoints.

const setOSSAccelIntegrityRuleBodyCap = 64 << 10

// setOSSAccelIntegrityRule handles POST /ossAccelIntegrityRule/set. Body is a
// proto.OSSAccelIntegrityRule; CreatedAt/UpdatedAt/LastRunAt/LastRunResult are
// server-controlled and reset here so a caller can't impersonate an
// existing record's run history.
func (m *Server) setOSSAccelIntegrityRule(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.OSSAccelIntegrityRuleSet))
	var err error
	defer func() { doStatAndMetric(proto.OSSAccelIntegrityRuleSet, metric, err, nil) }()

	body, rerr := io.ReadAll(io.LimitReader(r.Body, setOSSAccelIntegrityRuleBodyCap+1))
	if rerr != nil {
		err = rerr
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if len(body) > setOSSAccelIntegrityRuleBodyCap {
		err = fmt.Errorf("request body exceeds %d bytes", setOSSAccelIntegrityRuleBodyCap)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	rule := &proto.OSSAccelIntegrityRule{}
	if err = json.Unmarshal(body, rule); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if err = validateOSSAccelIntegrityRule(rule); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if _, err = m.cluster.getVol(rule.VolName); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeVolNotExists, Msg: err.Error()})
		return
	}

	existing := m.cluster.GetOSSAccelIntegrityRule(rule.VolName)
	now := time.Now()
	rule.UpdatedAt = now
	if existing != nil {
		rule.CreatedAt = existing.CreatedAt
		rule.LastRunAt = existing.LastRunAt
		rule.LastRunResult = existing.LastRunResult
	} else {
		rule.CreatedAt = now
	}

	if err = m.cluster.SetOSSAccelIntegrityRule(rule); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodePersistenceByRaft, Msg: err.Error()})
		return
	}
	AuditLog(r, "SetOSSAccelIntegrityRule", fmt.Sprintf("vol(%v) enabled(%v) intervalSeconds(%v) fullSampleCount(%v)",
		rule.VolName, rule.Enabled, rule.IntervalSeconds, rule.FullSampleCount), nil)
	sendOkReply(w, r, newSuccessHTTPReply(rule))
}

// getOSSAccelIntegrityRule handles GET /ossAccelIntegrityRule/get?name=<vol>.
func (m *Server) getOSSAccelIntegrityRule(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.OSSAccelIntegrityRuleGet))
	var err error
	defer func() { doStatAndMetric(proto.OSSAccelIntegrityRuleGet, metric, err, nil) }()

	name, err := parseAndExtractName(r)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if _, err = m.cluster.getVol(name); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeVolNotExists, Msg: err.Error()})
		return
	}
	rule := m.cluster.GetOSSAccelIntegrityRule(name)
	if rule == nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: fmt.Sprintf("no oss-accel integrity rule for vol %q", name)})
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(rule))
}

// deleteOSSAccelIntegrityRule handles POST /ossAccelIntegrityRule/delete?name=<vol>.
func (m *Server) deleteOSSAccelIntegrityRule(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.OSSAccelIntegrityRuleDelete))
	var err error
	defer func() { doStatAndMetric(proto.OSSAccelIntegrityRuleDelete, metric, err, nil) }()

	name, err := parseAndExtractName(r)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if _, err = m.cluster.getVol(name); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeVolNotExists, Msg: err.Error()})
		return
	}
	err = m.cluster.DeleteOSSAccelIntegrityRule(name)
	AuditLog(r, "DeleteOSSAccelIntegrityRule", fmt.Sprintf("vol(%v)", name), err)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
		return
	}
	msg := fmt.Sprintf("delete vol[%v] oss-accel integrity rule successfully", name)
	log.LogWarn(msg)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

// validateOSSAccelIntegrityRule checks the fields a caller controls directly.
func validateOSSAccelIntegrityRule(rule *proto.OSSAccelIntegrityRule) error {
	if rule.VolName == "" {
		return fmt.Errorf("volName is required")
	}
	if rule.IntervalSeconds == 0 {
		return fmt.Errorf("intervalSeconds must be > 0")
	}
	return nil
}
