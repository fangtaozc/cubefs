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

// Master HTTP admin surface for the M2 oss-accel changelog rule — one
// schedule per volume, mirrors SetBucketLifecycle / GetBucketLifecycle /
// DelBucketLifecycle (master/api_service.go:8379-8492) rather than
// SyncRule's independent-ID CRUD family: there's no separate rule ID,
// List, Pause, or Resume surface — "paused" is just Enabled=false on the
// same per-vol record, set via the same Set endpoint.

// setOSSAccelChangelogRuleBodyCap caps the JSON body size for /set so a
// hostile caller can't OOM master. The schema is tiny; 64 KiB is generous.
const setOSSAccelChangelogRuleBodyCap = 64 << 10

// setOSSAccelChangelogRule handles POST /ossAccelChangelogRule/set. Body is
// a proto.OSSAccelChangelogRule; CreatedAt/UpdatedAt/LastRunAt/LastRunResult
// are server-controlled and reset here so a caller can't impersonate an
// existing record's run history.
func (m *Server) setOSSAccelChangelogRule(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.OSSAccelChangelogRuleSet))
	var err error
	defer func() { doStatAndMetric(proto.OSSAccelChangelogRuleSet, metric, err, nil) }()

	body, rerr := io.ReadAll(io.LimitReader(r.Body, setOSSAccelChangelogRuleBodyCap+1))
	if rerr != nil {
		err = rerr
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if len(body) > setOSSAccelChangelogRuleBodyCap {
		err = fmt.Errorf("request body exceeds %d bytes", setOSSAccelChangelogRuleBodyCap)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	rule := &proto.OSSAccelChangelogRule{}
	if err = json.Unmarshal(body, rule); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if err = validateOSSAccelChangelogRule(rule); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if _, err = m.cluster.getVol(rule.VolName); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeVolNotExists, Msg: err.Error()})
		return
	}

	existing := m.cluster.GetOSSAccelChangelogRule(rule.VolName)
	now := time.Now()
	rule.UpdatedAt = now
	if existing != nil {
		rule.CreatedAt = existing.CreatedAt
		rule.LastRunAt = existing.LastRunAt
		rule.LastRunResult = existing.LastRunResult
	} else {
		rule.CreatedAt = now
	}

	if err = m.cluster.SetOSSAccelChangelogRule(rule); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodePersistenceByRaft, Msg: err.Error()})
		return
	}
	AuditLog(r, "SetOSSAccelChangelogRule", fmt.Sprintf("vol(%v) enabled(%v) intervalSeconds(%v)", rule.VolName, rule.Enabled, rule.IntervalSeconds), nil)
	sendOkReply(w, r, newSuccessHTTPReply(rule))
}

// getOSSAccelChangelogRule handles GET /ossAccelChangelogRule/get?name=<vol>.
func (m *Server) getOSSAccelChangelogRule(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.OSSAccelChangelogRuleGet))
	var err error
	defer func() { doStatAndMetric(proto.OSSAccelChangelogRuleGet, metric, err, nil) }()

	name, err := parseAndExtractName(r)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if _, err = m.cluster.getVol(name); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeVolNotExists, Msg: err.Error()})
		return
	}
	rule := m.cluster.GetOSSAccelChangelogRule(name)
	if rule == nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: fmt.Sprintf("no oss-accel changelog rule for vol %q", name)})
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(rule))
}

// deleteOSSAccelChangelogRule handles POST /ossAccelChangelogRule/delete?name=<vol>.
func (m *Server) deleteOSSAccelChangelogRule(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.OSSAccelChangelogRuleDelete))
	var err error
	defer func() { doStatAndMetric(proto.OSSAccelChangelogRuleDelete, metric, err, nil) }()

	name, err := parseAndExtractName(r)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if _, err = m.cluster.getVol(name); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeVolNotExists, Msg: err.Error()})
		return
	}
	err = m.cluster.DeleteOSSAccelChangelogRule(name)
	AuditLog(r, "DeleteOSSAccelChangelogRule", fmt.Sprintf("vol(%v)", name), err)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
		return
	}
	msg := fmt.Sprintf("delete vol[%v] oss-accel changelog rule successfully", name)
	log.LogWarn(msg)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

// validateOSSAccelChangelogRule checks the fields a caller controls
// directly (server-controlled timestamps are reset by the caller, not
// validated here).
func validateOSSAccelChangelogRule(rule *proto.OSSAccelChangelogRule) error {
	if rule.VolName == "" {
		return fmt.Errorf("volName is required")
	}
	if rule.IntervalSeconds == 0 {
		return fmt.Errorf("intervalSeconds must be > 0")
	}
	return nil
}
