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

// Master HTTP admin surface for the oss-accel audit rule — one schedule
// per volume, mirrors api_service_oss_accel_changelog_rule.go exactly.
// Auth level matches the changelog/eviction rule routes ("plain", i.e. not
// wrapped in requireSyncAdminToken) — this is master's own rule CRUD
// surface, a different auth domain from lcnode's own HTTP endpoints.

const setOSSAccelAuditRuleBodyCap = 64 << 10

// setOSSAccelAuditRule handles POST /ossAccelAuditRule/set. Body is a
// proto.OSSAccelAuditRule; CreatedAt/UpdatedAt/LastRunAt/LastRunResult are
// server-controlled and reset here so a caller can't impersonate an
// existing record's run history.
func (m *Server) setOSSAccelAuditRule(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.OSSAccelAuditRuleSet))
	var err error
	defer func() { doStatAndMetric(proto.OSSAccelAuditRuleSet, metric, err, nil) }()

	body, rerr := io.ReadAll(io.LimitReader(r.Body, setOSSAccelAuditRuleBodyCap+1))
	if rerr != nil {
		err = rerr
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if len(body) > setOSSAccelAuditRuleBodyCap {
		err = fmt.Errorf("request body exceeds %d bytes", setOSSAccelAuditRuleBodyCap)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	rule := &proto.OSSAccelAuditRule{}
	if err = json.Unmarshal(body, rule); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if err = validateOSSAccelAuditRule(rule); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if _, err = m.cluster.getVol(rule.VolName); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeVolNotExists, Msg: err.Error()})
		return
	}

	existing := m.cluster.GetOSSAccelAuditRule(rule.VolName)
	now := time.Now()
	rule.UpdatedAt = now
	if existing != nil {
		rule.CreatedAt = existing.CreatedAt
		rule.LastRunAt = existing.LastRunAt
		rule.LastRunResult = existing.LastRunResult
	} else {
		rule.CreatedAt = now
	}

	if err = m.cluster.SetOSSAccelAuditRule(rule); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodePersistenceByRaft, Msg: err.Error()})
		return
	}
	AuditLog(r, "SetOSSAccelAuditRule", fmt.Sprintf("vol(%v) enabled(%v) intervalSeconds(%v)", rule.VolName, rule.Enabled, rule.IntervalSeconds), nil)
	sendOkReply(w, r, newSuccessHTTPReply(rule))
}

// getOSSAccelAuditRule handles GET /ossAccelAuditRule/get?name=<vol>.
func (m *Server) getOSSAccelAuditRule(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.OSSAccelAuditRuleGet))
	var err error
	defer func() { doStatAndMetric(proto.OSSAccelAuditRuleGet, metric, err, nil) }()

	name, err := parseAndExtractName(r)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if _, err = m.cluster.getVol(name); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeVolNotExists, Msg: err.Error()})
		return
	}
	rule := m.cluster.GetOSSAccelAuditRule(name)
	if rule == nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: fmt.Sprintf("no oss-accel audit rule for vol %q", name)})
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(rule))
}

// deleteOSSAccelAuditRule handles POST /ossAccelAuditRule/delete?name=<vol>.
func (m *Server) deleteOSSAccelAuditRule(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.OSSAccelAuditRuleDelete))
	var err error
	defer func() { doStatAndMetric(proto.OSSAccelAuditRuleDelete, metric, err, nil) }()

	name, err := parseAndExtractName(r)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if _, err = m.cluster.getVol(name); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeVolNotExists, Msg: err.Error()})
		return
	}
	err = m.cluster.DeleteOSSAccelAuditRule(name)
	AuditLog(r, "DeleteOSSAccelAuditRule", fmt.Sprintf("vol(%v)", name), err)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
		return
	}
	msg := fmt.Sprintf("delete vol[%v] oss-accel audit rule successfully", name)
	log.LogWarn(msg)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

// validateOSSAccelAuditRule checks the fields a caller controls directly.
func validateOSSAccelAuditRule(rule *proto.OSSAccelAuditRule) error {
	if rule.VolName == "" {
		return fmt.Errorf("volName is required")
	}
	if rule.IntervalSeconds == 0 {
		return fmt.Errorf("intervalSeconds must be > 0")
	}
	return nil
}
