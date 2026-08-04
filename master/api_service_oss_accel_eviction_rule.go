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

// Master HTTP admin surface for the M3 oss-accel eviction rule — mirrors
// api_service_oss_accel_changelog_rule.go exactly (one schedule per volume,
// plain Set/Get/Delete, no ID/List/Pause/Resume surface).

const setOSSAccelEvictionRuleBodyCap = 64 << 10

func (m *Server) setOSSAccelEvictionRule(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.OSSAccelEvictionRuleSet))
	var err error
	defer func() { doStatAndMetric(proto.OSSAccelEvictionRuleSet, metric, err, nil) }()

	body, rerr := io.ReadAll(io.LimitReader(r.Body, setOSSAccelEvictionRuleBodyCap+1))
	if rerr != nil {
		err = rerr
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if len(body) > setOSSAccelEvictionRuleBodyCap {
		err = fmt.Errorf("request body exceeds %d bytes", setOSSAccelEvictionRuleBodyCap)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	rule := &proto.OSSAccelEvictionRule{}
	if err = json.Unmarshal(body, rule); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if err = validateOSSAccelEvictionRule(rule); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if _, err = m.cluster.getVol(rule.VolName); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeVolNotExists, Msg: err.Error()})
		return
	}

	existing := m.cluster.GetOSSAccelEvictionRule(rule.VolName)
	now := time.Now()
	rule.UpdatedAt = now
	if existing != nil {
		rule.CreatedAt = existing.CreatedAt
		rule.LastRunAt = existing.LastRunAt
		rule.LastRunResult = existing.LastRunResult
		rule.EvictionInFlight = existing.EvictionInFlight
	} else {
		rule.CreatedAt = now
		rule.EvictionInFlight = false
	}

	if err = m.cluster.SetOSSAccelEvictionRule(rule); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodePersistenceByRaft, Msg: err.Error()})
		return
	}
	AuditLog(r, "SetOSSAccelEvictionRule", fmt.Sprintf("vol(%v) enabled(%v) high(%v) low(%v)", rule.VolName, rule.Enabled, rule.HighWatermarkRatio, rule.LowWatermarkRatio), nil)
	sendOkReply(w, r, newSuccessHTTPReply(rule))
}

func (m *Server) getOSSAccelEvictionRule(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.OSSAccelEvictionRuleGet))
	var err error
	defer func() { doStatAndMetric(proto.OSSAccelEvictionRuleGet, metric, err, nil) }()

	name, err := parseAndExtractName(r)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if _, err = m.cluster.getVol(name); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeVolNotExists, Msg: err.Error()})
		return
	}
	rule := m.cluster.GetOSSAccelEvictionRule(name)
	if rule == nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: fmt.Sprintf("no oss-accel eviction rule for vol %q", name)})
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(rule))
}

func (m *Server) deleteOSSAccelEvictionRule(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.OSSAccelEvictionRuleDelete))
	var err error
	defer func() { doStatAndMetric(proto.OSSAccelEvictionRuleDelete, metric, err, nil) }()

	name, err := parseAndExtractName(r)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if _, err = m.cluster.getVol(name); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeVolNotExists, Msg: err.Error()})
		return
	}
	err = m.cluster.DeleteOSSAccelEvictionRule(name)
	AuditLog(r, "DeleteOSSAccelEvictionRule", fmt.Sprintf("vol(%v)", name), err)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
		return
	}
	msg := fmt.Sprintf("delete vol[%v] oss-accel eviction rule successfully", name)
	log.LogWarn(msg)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

func validateOSSAccelEvictionRule(rule *proto.OSSAccelEvictionRule) error {
	if rule.VolName == "" {
		return fmt.Errorf("volName is required")
	}
	if rule.HighWatermarkRatio <= 0 || rule.HighWatermarkRatio > 1 {
		return fmt.Errorf("highWatermarkRatio must be in (0, 1]")
	}
	if rule.LowWatermarkRatio <= 0 || rule.LowWatermarkRatio >= rule.HighWatermarkRatio {
		return fmt.Errorf("lowWatermarkRatio must be in (0, highWatermarkRatio)")
	}
	// 对齐AFM(eviction排序策略): empty is valid (defaults to lastRecall,
	// the pre-existing behavior) — only reject an unrecognized non-empty
	// value, so a typo doesn't silently fall back to the default instead of
	// erroring loudly.
	switch rule.Order {
	case "", proto.OSSAccelEvictionOrderLastRecall, proto.OSSAccelEvictionOrderSize:
	default:
		return fmt.Errorf("order must be %q or %q (or omitted)", proto.OSSAccelEvictionOrderLastRecall, proto.OSSAccelEvictionOrderSize)
	}
	return nil
}
