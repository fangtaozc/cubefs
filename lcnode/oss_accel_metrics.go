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

// 系统层面收尾续(补可观测性) — oss-accel's operations (flush/recall/
// commitCold/audit/trashPurge/changelogSync/evict/flushPolicy/integrity)
// were log-only until now: no counts, latencies, or failure rates existed
// anywhere, so a production incident could only be diagnosed by grepping
// logs. This file wires them into the SAME exporter infrastructure every
// other CubeFS component already uses (util/exporter) — no new metrics
// system, no new dependency.
//
// Naming follows objectnode's existing action_%v convention
// (objectnode/api_middleware.go:127) and master's exporter.Vol-keyed
// labeling convention (master/api_service_user.go:222) — final Prometheus
// names come out as cfs_lcNode_action_<op>_count / _hist_bucket|sum|count /
// _failed, matching docs/source/ecology/prometheus.md's documented
// cfs_<module>_$op_* scheme.
package lcnode

import (
	"errors"
	"net/http"

	"github.com/cubefs/cubefs/util/exporter"
)

// errOssAccelHTTPFailed is a placeholder passed to TimePointCount.SetWithLabels
// to signal "this request failed" — ossAccelObserveHTTP only has a status
// code, not a real Go error, but SetWithLabels' UMP-alarm path just checks
// err != nil, so any non-nil sentinel works.
var errOssAccelHTTPFailed = errors.New("oss-accel http response status >= 400")

// ossAccelObserve starts a latency+count metric for one oss-accel operation
// and returns a func to defer. The returned func reads *errp at UNWIND time
// (not at call time), so the standard idiom is:
//
//	func runOssAccelFlushForVol(vol, path string, ...) (s3key, checksum string, err error) {
//	    defer ossAccelObserve("flush", vol, &err)()
//	    ...
//	}
//
// relying on Go's guarantee that a named return value is fully settled
// before deferred funcs run. On top of the latency histogram + count
// (TimePointCount.SetWithLabels), a non-nil *errp also bumps an explicit
// action_<op>_failed counter — mirroring objectnode's separate failed_%v
// counter (api_middleware.go:148) rather than relying solely on the
// TimePointCount's internal UMP alarm-on-error, so failure rate is a
// directly queryable Prometheus metric, not just an internal alarm.
func ossAccelObserve(op, vol string, errp *error) func() {
	metric := exporter.NewTPCnt("action_" + op)
	return func() {
		metric.SetWithLabels(*errp, map[string]string{exporter.Vol: vol})
		if *errp != nil {
			exporter.NewCounter("action_"+op+"_failed").AddWithLabels(1, map[string]string{exporter.Vol: vol})
		}
	}
}

// statusCapturingWriter intercepts WriteHeader so ossAccelObserveHTTP can
// classify success/failure by response status without touching the wrapped
// handler's internals at all — recall (unlike every other oss-accel
// operation) has no single "core function vs HTTP handler" split to hook a
// defer into; its many http.Error early-returns use locally-scoped error
// variables (aerr/gerr/...) by design, from real-machine-validated
// concurrency/migration-slot handling that shouldn't be touched just to
// thread a metric through. A response-status wrapper needs zero changes to
// any of that.
type statusCapturingWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusCapturingWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

// ossAccelObserveHTTP wraps an oss-accel HTTP handler with the same
// latency+count+failure-counter shape as ossAccelObserve, classifying
// failure as "response status >= 400" — the same technique objectnode's own
// traceMiddleware already uses (objectnode/api_middleware.go:127-130,148),
// just scoped to a single route instead of a global middleware chain (lcnode
// has no shared middleware chain the way objectnode's mux does). vol is read
// from the form value; ParseForm is idempotent, so calling it here doesn't
// affect the wrapped handler's own later ParseForm call.
func ossAccelObserveHTTP(op string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		vol := r.FormValue("vol")
		sw := &statusCapturingWriter{ResponseWriter: w, status: http.StatusOK}
		metric := exporter.NewTPCnt("action_" + op)
		next(sw, r)
		failed := sw.status >= http.StatusBadRequest
		var errForMetric error
		if failed {
			errForMetric = errOssAccelHTTPFailed
		}
		metric.SetWithLabels(errForMetric, map[string]string{exporter.Vol: vol})
		if failed {
			exporter.NewCounter("action_"+op+"_failed").AddWithLabels(1, map[string]string{exporter.Vol: vol})
		}
	}
}
