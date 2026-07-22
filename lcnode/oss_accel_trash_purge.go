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

// Retention policy for AUDIT-2's .trash/ quarantine — the other half of
// "never destroy, only rename first" (M1 delayed-release grace period, M4
// relocate's fail-closed conflict check, AUDIT-2's own quarantine-not-delete
// stance): quarantining forever isn't cleanup, it just moves the leak to a
// different prefix. This is the deliberate, bounded second step that
// actually reclaims the space, once a quarantined object has sat unclaimed
// long enough that nobody is coming to rename it back.
package lcnode

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/cubefs/cubefs/syncnode/backend"
	"github.com/cubefs/cubefs/syncnode/backend/s3"
	"github.com/cubefs/cubefs/util/log"
)

// ossAccelDefaultTrashRetentionHours is how long a quarantined object sits
// under .trash/ before this endpoint will permanently delete it. This stacks
// on top of AUDIT-2's own orphanGraceHours (default 24h) as a second,
// independent safety margin — total time from "first seen unreferenced" to
// "gone forever" is orphanGraceHours + retentionHours by default (24h +
// 168h). Overridable per-call via retentionHours; not a per-vol persisted
// setting, matching every other oss-accel endpoint's manual-first stance.
const ossAccelDefaultTrashRetentionHours = 24 * 7

// httpServiceOssAccelTrashPurge handles GET /ossAccelTrashPurge?vol=&prefix=&retentionHours=
// prefix is interpreted the same way AUDIT does: it's the ORIGINAL key
// prefix (before quarantine prepended .trash/), not a path within .trash/
// itself — callers never need to know the internal .trash/ layout.
func (l *LcNode) httpServiceOssAccelTrashPurge(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, fmt.Sprintf("ParseForm err: %v", err), http.StatusBadRequest)
		return
	}
	vol := r.FormValue("vol")
	if vol == "" {
		http.Error(w, "missing required form value: vol", http.StatusBadRequest)
		return
	}
	prefix := r.FormValue("prefix")
	retentionHours := parseUintForm(r, "retentionHours", ossAccelDefaultTrashRetentionHours)

	metaWrapper, err := l.buildVolMetaWrapper(vol)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer metaWrapper.Close()

	s3Cfg, err := loadOssAccelS3Config(metaWrapper, vol)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	s3Backend, err := s3.New(s3Cfg)
	if err != nil {
		http.Error(w, fmt.Sprintf("s3 backend init err: %v", err), http.StatusInternalServerError)
		return
	}
	defer s3Backend.Close()

	purged, perr := runOssAccelTrashPurge(s3Backend, prefix, time.Duration(retentionHours)*time.Hour)
	if perr != nil {
		http.Error(w, perr.Error(), http.StatusInternalServerError)
		return
	}
	fmt.Fprintf(w, "ok: vol=%v prefix=%v retentionHours=%v purged=%v\npurgedKeys=%v\n",
		vol, prefix, retentionHours, len(purged), purged)
}

// runOssAccelTrashPurge deletes every object under .trash/<prefix> whose S3
// Mtime is at least retention old. Mtime doubles as "time quarantined"
// here: Backend.Rename (AUDIT-2's quarantine step) is CopyObject+Delete, and
// S3 always stamps a fresh LastModified on the CopyObject destination
// regardless of metadata directive — a guaranteed S3 semantic, not an
// implementation detail that could silently change. oss-accel's own Put
// calls never set PutOptions.Mtime, so a quarantined object carries no
// competing source-mtime metadata that would override this. Returns the
// keys actually deleted (with the .trash/ prefix stripped back off, so
// callers see the ORIGINAL key that leaked — matching AUDIT-2's
// QuarantinedKeys reporting convention) plus any partial progress if an
// error interrupts the listing.
func runOssAccelTrashPurge(s3Backend backend.Backend, prefix string, retention time.Duration) (purged []string, err error) {
	ctx := context.Background()
	ch, lerr := s3Backend.List(ctx, ossAccelTrashPrefix+prefix, true)
	if lerr != nil {
		return nil, fmt.Errorf("s3 list err: %v", lerr)
	}
	now := time.Now()
	for entry := range ch {
		if entry.Err != nil {
			return purged, fmt.Errorf("s3 list entry err: %v", entry.Err)
		}
		if entry.IsDir {
			continue
		}
		if now.Sub(entry.Mtime) < retention {
			continue // not old enough yet — give a false positive time to be renamed back
		}
		if derr := s3Backend.Delete(ctx, entry.Key); derr != nil {
			log.LogWarnf("runOssAccelTrashPurge: failed to delete key(%v): %v", entry.Key, derr)
			continue
		}
		originalKey := strings.TrimPrefix(entry.Key, ossAccelTrashPrefix)
		purged = append(purged, originalKey)
		log.LogWarnf("runOssAccelTrashPurge: purged key(%v) (quarantined %v ago, >= retention %v)", originalKey, now.Sub(entry.Mtime), retention)
	}
	return purged, nil
}
