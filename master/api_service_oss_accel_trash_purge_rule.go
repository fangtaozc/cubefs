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

// Master HTTP admin surface for the oss-accel trash purge rule — one
// schedule per volume, mirrors api_service_oss_accel_audit_rule.go exactly.

const setOSSAccelTrashPurgeRuleBodyCap = 64 << 10

// setOSSAccelTrashPurgeRule handles POST /ossAccelTrashPurgeRule/set.
func (m *Server) setOSSAccelTrashPurgeRule(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.OSSAccelTrashPurgeRuleSet))
	var err error
	defer func() { doStatAndMetric(proto.OSSAccelTrashPurgeRuleSet, metric, err, nil) }()

	body, rerr := io.ReadAll(io.LimitReader(r.Body, setOSSAccelTrashPurgeRuleBodyCap+1))
	if rerr != nil {
		err = rerr
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if len(body) > setOSSAccelTrashPurgeRuleBodyCap {
		err = fmt.Errorf("request body exceeds %d bytes", setOSSAccelTrashPurgeRuleBodyCap)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	rule := &proto.OSSAccelTrashPurgeRule{}
	if err = json.Unmarshal(body, rule); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if err = validateOSSAccelTrashPurgeRule(rule); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if _, err = m.cluster.getVol(rule.VolName); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeVolNotExists, Msg: err.Error()})
		return
	}

	existing := m.cluster.GetOSSAccelTrashPurgeRule(rule.VolName)
	now := time.Now()
	rule.UpdatedAt = now
	if existing != nil {
		rule.CreatedAt = existing.CreatedAt
		rule.LastRunAt = existing.LastRunAt
		rule.LastRunResult = existing.LastRunResult
	} else {
		rule.CreatedAt = now
	}

	if err = m.cluster.SetOSSAccelTrashPurgeRule(rule); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodePersistenceByRaft, Msg: err.Error()})
		return
	}
	AuditLog(r, "SetOSSAccelTrashPurgeRule", fmt.Sprintf("vol(%v) enabled(%v) intervalSeconds(%v)", rule.VolName, rule.Enabled, rule.IntervalSeconds), nil)
	sendOkReply(w, r, newSuccessHTTPReply(rule))
}

// getOSSAccelTrashPurgeRule handles GET /ossAccelTrashPurgeRule/get?name=<vol>.
func (m *Server) getOSSAccelTrashPurgeRule(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.OSSAccelTrashPurgeRuleGet))
	var err error
	defer func() { doStatAndMetric(proto.OSSAccelTrashPurgeRuleGet, metric, err, nil) }()

	name, err := parseAndExtractName(r)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if _, err = m.cluster.getVol(name); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeVolNotExists, Msg: err.Error()})
		return
	}
	rule := m.cluster.GetOSSAccelTrashPurgeRule(name)
	if rule == nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: fmt.Sprintf("no oss-accel trash purge rule for vol %q", name)})
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(rule))
}

// deleteOSSAccelTrashPurgeRule handles POST /ossAccelTrashPurgeRule/delete?name=<vol>.
func (m *Server) deleteOSSAccelTrashPurgeRule(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.OSSAccelTrashPurgeRuleDelete))
	var err error
	defer func() { doStatAndMetric(proto.OSSAccelTrashPurgeRuleDelete, metric, err, nil) }()

	name, err := parseAndExtractName(r)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if _, err = m.cluster.getVol(name); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeVolNotExists, Msg: err.Error()})
		return
	}
	err = m.cluster.DeleteOSSAccelTrashPurgeRule(name)
	AuditLog(r, "DeleteOSSAccelTrashPurgeRule", fmt.Sprintf("vol(%v)", name), err)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
		return
	}
	msg := fmt.Sprintf("delete vol[%v] oss-accel trash purge rule successfully", name)
	log.LogWarn(msg)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

// validateOSSAccelTrashPurgeRule checks the fields a caller controls directly.
func validateOSSAccelTrashPurgeRule(rule *proto.OSSAccelTrashPurgeRule) error {
	if rule.VolName == "" {
		return fmt.Errorf("volName is required")
	}
	if rule.IntervalSeconds == 0 {
		return fmt.Errorf("intervalSeconds must be > 0")
	}
	return nil
}
