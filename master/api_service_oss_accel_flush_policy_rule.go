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

// Master HTTP admin surface for the oss-accel flush-policy rule — one schedule
// per volume, mirrors api_service_oss_accel_audit_rule.go exactly.
// Auth level matches the changelog/eviction rule routes ("plain", i.e. not
// wrapped in requireSyncAdminToken) — this is master's own rule CRUD
// surface, a different auth domain from lcnode's own HTTP endpoints.

const setOSSAccelFlushPolicyRuleBodyCap = 64 << 10

// setOSSAccelFlushPolicyRule handles POST /ossAccelFlushPolicyRule/set. Body is a
// proto.OSSAccelFlushPolicyRule; CreatedAt/UpdatedAt/LastRunAt/LastRunResult are
// server-controlled and reset here so a caller can't impersonate an
// existing record's run history.
func (m *Server) setOSSAccelFlushPolicyRule(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.OSSAccelFlushPolicyRuleSet))
	var err error
	defer func() { doStatAndMetric(proto.OSSAccelFlushPolicyRuleSet, metric, err, nil) }()

	body, rerr := io.ReadAll(io.LimitReader(r.Body, setOSSAccelFlushPolicyRuleBodyCap+1))
	if rerr != nil {
		err = rerr
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if len(body) > setOSSAccelFlushPolicyRuleBodyCap {
		err = fmt.Errorf("request body exceeds %d bytes", setOSSAccelFlushPolicyRuleBodyCap)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	rule := &proto.OSSAccelFlushPolicyRule{}
	if err = json.Unmarshal(body, rule); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if err = validateOSSAccelFlushPolicyRule(rule); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if _, err = m.cluster.getVol(rule.VolName); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeVolNotExists, Msg: err.Error()})
		return
	}

	existing := m.cluster.GetOSSAccelFlushPolicyRule(rule.VolName)
	now := time.Now()
	rule.UpdatedAt = now
	if existing != nil {
		rule.CreatedAt = existing.CreatedAt
		rule.LastRunAt = existing.LastRunAt
		rule.LastRunResult = existing.LastRunResult
		rule.FlushPolicyInFlight = existing.FlushPolicyInFlight
	} else {
		rule.CreatedAt = now
		rule.FlushPolicyInFlight = false
	}

	if err = m.cluster.SetOSSAccelFlushPolicyRule(rule); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodePersistenceByRaft, Msg: err.Error()})
		return
	}
	AuditLog(r, "SetOSSAccelFlushPolicyRule", fmt.Sprintf("vol(%v) enabled(%v) intervalSeconds(%v) minIdleHours(%v) minSizeBytes(%v)",
		rule.VolName, rule.Enabled, rule.IntervalSeconds, rule.MinIdleHours, rule.MinSizeBytes), nil)
	sendOkReply(w, r, newSuccessHTTPReply(rule))
}

// getOSSAccelFlushPolicyRule handles GET /ossAccelFlushPolicyRule/get?name=<vol>.
func (m *Server) getOSSAccelFlushPolicyRule(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.OSSAccelFlushPolicyRuleGet))
	var err error
	defer func() { doStatAndMetric(proto.OSSAccelFlushPolicyRuleGet, metric, err, nil) }()

	name, err := parseAndExtractName(r)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if _, err = m.cluster.getVol(name); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeVolNotExists, Msg: err.Error()})
		return
	}
	rule := m.cluster.GetOSSAccelFlushPolicyRule(name)
	if rule == nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: fmt.Sprintf("no oss-accel flush-policy rule for vol %q", name)})
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(rule))
}

// deleteOSSAccelFlushPolicyRule handles POST /ossAccelFlushPolicyRule/delete?name=<vol>.
func (m *Server) deleteOSSAccelFlushPolicyRule(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.OSSAccelFlushPolicyRuleDelete))
	var err error
	defer func() { doStatAndMetric(proto.OSSAccelFlushPolicyRuleDelete, metric, err, nil) }()

	name, err := parseAndExtractName(r)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if _, err = m.cluster.getVol(name); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeVolNotExists, Msg: err.Error()})
		return
	}
	err = m.cluster.DeleteOSSAccelFlushPolicyRule(name)
	AuditLog(r, "DeleteOSSAccelFlushPolicyRule", fmt.Sprintf("vol(%v)", name), err)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
		return
	}
	msg := fmt.Sprintf("delete vol[%v] oss-accel flush-policy rule successfully", name)
	log.LogWarn(msg)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

// validateOSSAccelFlushPolicyRule checks the fields a caller controls directly.
func validateOSSAccelFlushPolicyRule(rule *proto.OSSAccelFlushPolicyRule) error {
	if rule.VolName == "" {
		return fmt.Errorf("volName is required")
	}
	if rule.IntervalSeconds == 0 {
		return fmt.Errorf("intervalSeconds must be > 0")
	}
	if rule.MinIdleHours == 0 {
		return fmt.Errorf("minIdleHours must be > 0 — 0 would flush every never-tiered file on the very next sweep")
	}
	return nil
}
