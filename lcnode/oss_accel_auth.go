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

// 系统层面收尾: shared-token gate for lcnode's 7 oss-accel HTTP endpoints
// (previously zero authentication — anyone able to reach lcnode's port
// could trigger flush/recall/commitCold/changelogSync/relocate/audit/
// trashPurge, including a genuine destructive operation). Structurally
// identical to syncnode/api/api.go's AuthMiddleware and
// master/sync_node_auth.go's requireSyncAdminToken — this is the one
// existing precedent for "gate an internal admin HTTP surface with an
// optional shared token" in this codebase, not a new pattern.
//
// The token is installed at server startup via SetLcnodeAdminToken. When
// EMPTY the middleware is a passthrough — preserves zero-config dev/test
// behavior for every deployment that never sets it. Wire format:
// `Authorization: Bearer <token>` or `X-Lcnode-Admin-Token: <token>`.
// Compare is constant-time so a network attacker can't derive the token
// from response-time differences.
package lcnode

import (
	"crypto/subtle"
	"net/http"
	"strings"
	"sync"
)

var (
	lcnodeAdminTokenMu sync.RWMutex
	lcnodeAdminToken   string
)

// SetLcnodeAdminToken installs the admin token. Passing an empty string
// disables auth. Safe to call concurrently.
func SetLcnodeAdminToken(t string) {
	lcnodeAdminTokenMu.Lock()
	lcnodeAdminToken = t
	lcnodeAdminTokenMu.Unlock()
}

func currentLcnodeAdminToken() string {
	lcnodeAdminTokenMu.RLock()
	defer lcnodeAdminTokenMu.RUnlock()
	return lcnodeAdminToken
}

// lcnodeConstantTimeEq mirrors syncnode/api/api.go's constantTimeEq exactly
// (same length-leak defense: a dummy compare on mismatched lengths so
// "wrong length" and "right length, wrong value" take the same time).
func lcnodeConstantTimeEq(a, b string) bool {
	if len(a) != len(b) {
		_ = subtle.ConstantTimeCompare([]byte(a), []byte(a))
		return false
	}
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

// extractLcnodeAdminToken mirrors syncnode/api/api.go's extractToken.
func extractLcnodeAdminToken(r *http.Request) string {
	if v := r.Header.Get("Authorization"); v != "" {
		const bearer = "bearer "
		if len(v) > len(bearer) && strings.EqualFold(v[:len(bearer)], bearer) {
			return strings.TrimSpace(v[len(bearer):])
		}
	}
	return strings.TrimSpace(r.Header.Get("X-Lcnode-Admin-Token"))
}

// requireLcnodeAdminToken wraps an oss-accel HTTP handler with the shared-
// token gate. Empty configured token = passthrough.
func requireLcnodeAdminToken(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tok := currentLcnodeAdminToken()
		if tok == "" {
			next(w, r)
			return
		}
		provided := extractLcnodeAdminToken(r)
		if provided == "" || !lcnodeConstantTimeEq(provided, tok) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next(w, r)
	}
}
