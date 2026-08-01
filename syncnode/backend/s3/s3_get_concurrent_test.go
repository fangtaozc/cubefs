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

package s3

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/cubefs/cubefs/syncnode/backend"
)

// recordingWriterAt is an io.WriterAt that records every call (offset +
// length) instead of actually storing bytes anywhere — enough to assert on
// chunk boundaries (no overlap, no gap, exact total coverage) without
// needing a real ExtentClient, which has no mock in this repo (see plan
// doc). Also verifies the written bytes against a reference buffer, so a
// bug that writes the right length at the wrong offset (or vice versa)
// still fails the test.
type recordingWriterAt struct {
	mu    sync.Mutex
	calls []writeAtCall
	want  []byte // reference: the object's full expected content
	bad   []string
}

type writeAtCall struct {
	off int64
	n   int
}

func (r *recordingWriterAt) WriteAt(p []byte, off int64) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, writeAtCall{off: off, n: len(p)})
	end := off + int64(len(p))
	if end > int64(len(r.want)) {
		r.bad = append(r.bad, fmt.Sprintf("WriteAt off=%d len=%d exceeds want len=%d", off, len(p), len(r.want)))
		return len(p), nil
	}
	for i, b := range p {
		if r.want[int(off)+i] != b {
			r.bad = append(r.bad, fmt.Sprintf("content mismatch at global offset %d", int(off)+i))
			break
		}
	}
	return len(p), nil
}

// assertFullCoverage sorts recorded calls by offset and checks the union of
// [off, off+n) ranges is exactly [0, wantLen) with no overlap and no gap —
// the property GetConcurrent's caller (runOssAccelRecallWrite) depends on:
// every byte of the object must land exactly once.
func (r *recordingWriterAt) assertFullCoverage(t *testing.T, wantLen int) {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, b := range r.bad {
		t.Errorf("recordingWriterAt: %s", b)
	}
	if len(r.calls) == 0 {
		t.Fatal("no WriteAt calls recorded")
	}

	calls := append([]writeAtCall(nil), r.calls...)
	sort.Slice(calls, func(i, j int) bool { return calls[i].off < calls[j].off })

	var cursor int64
	for _, c := range calls {
		if c.off != cursor {
			t.Fatalf("gap/overlap in WriteAt coverage: expected next call at offset %d, got offset %d (call %+v)", cursor, c.off, c)
		}
		cursor += int64(c.n)
	}
	if cursor != int64(wantLen) {
		t.Fatalf("WriteAt coverage total = %d bytes, want %d", cursor, wantLen)
	}
}

func referenceBody(size int) []byte {
	body := make([]byte, size)
	for i := range body {
		body[i] = byte(i % 251)
	}
	return body
}

func TestGetConcurrent_SmallObject(t *testing.T) {
	t.Parallel()
	m := newMockS3("test-bucket")
	defer m.close()
	b := newTestBackend(t, m)
	defer b.Close()

	ctx := context.Background()
	body := referenceBody(1024)
	if _, err := b.Put(ctx, "small.bin", strings.NewReader(string(body)), int64(len(body)), backend.PutOptions{}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	w := &recordingWriterAt{want: body}
	if err := b.GetConcurrent(ctx, "small.bin", w, 4); err != nil {
		t.Fatalf("GetConcurrent: %v", err)
	}
	w.assertFullCoverage(t, len(body))
}

// TestGetConcurrent_MultiPartConcurrency uses an object large enough (across
// several PartSizeMiB=5 parts, from newTestBackend's Config) to force
// manager.Downloader to actually split into multiple concurrent range-fetch
// workers, not degrade to a single GetObject.
func TestGetConcurrent_MultiPartConcurrency(t *testing.T) {
	t.Parallel()
	m := newMockS3("test-bucket")
	defer m.close()
	b := newTestBackend(t, m)
	defer b.Close()

	ctx := context.Background()
	size := 22 * mib // > 4 parts at PartSizeMiB=5
	body := referenceBody(size)
	if _, err := b.Put(ctx, "big.bin", strings.NewReader(string(body)), int64(len(body)), backend.PutOptions{}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	w := &recordingWriterAt{want: body}
	if err := b.GetConcurrent(ctx, "big.bin", w, 8); err != nil {
		t.Fatalf("GetConcurrent: %v", err)
	}
	w.assertFullCoverage(t, size)

	w.mu.Lock()
	numCalls := len(w.calls)
	w.mu.Unlock()
	// One WriteAt per part (BufferProvider sized to PartSizeMiB — see
	// GetConcurrent's doc comment) — not one per part *plus* a fragmented
	// tail from a too-small default buffer. Exact call count depends on the
	// SDK's internal chunking, so this only asserts an upper bound loose
	// enough to catch a regression to the default (nil) BufferProvider,
	// which would produce call counts in the thousands for a 22 MiB object.
	if numCalls > 50 {
		t.Errorf("WriteAt called %d times for a %d-byte object with PartSizeMiB=5 BufferProvider — "+
			"expected O(part count), got O(default 32KB buffer); BufferProvider likely not wired", numCalls, size)
	}
}

// TestGetConcurrent_PartialFailurePropagates verifies that when one of
// several concurrent range-fetch workers hits a server error, the overall
// GetConcurrent call returns an error rather than silently succeeding with
// a partially-written object. This is the property runOssAccelRecallWrite's
// "all-or-nothing" contract depends on (see design doc / plan) — a partial
// download must never look like a successful recall.
func TestGetConcurrent_PartialFailurePropagates(t *testing.T) {
	t.Parallel()
	m := newMockS3("test-bucket")
	defer m.close()
	b := newTestBackend(t, m)
	defer b.Close()

	ctx := context.Background()
	size := 22 * mib
	body := referenceBody(size)
	if _, err := b.Put(ctx, "big.bin", strings.NewReader(string(body)), int64(len(body)), backend.PutOptions{}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Fail the second part's request exactly — the Downloader issues Range
	// requests starting precisely at part boundaries (n * PartSizeMiB), so
	// injecting failure on an arbitrary sub-range (e.g. [6MiB, 9MiB)) that
	// doesn't align with a boundary never actually triggers, silently
	// passing the test for the wrong reason. PartSizeMiB=5 here (see
	// newTestBackend), so the second part starts at exactly 5 MiB — one of
	// several concurrent workers, not the first (which could trivially
	// short-circuit before others even start).
	failStart := int64(5 * mib)
	m.failRangeStart = func(start int64) bool { return start == failStart }

	w := &recordingWriterAt{want: body}
	err := b.GetConcurrent(ctx, "big.bin", w, 8)
	if err == nil {
		t.Fatal("expected GetConcurrent to return an error when a range-fetch worker fails, got nil")
	}
}

func TestGetConcurrent_NotFound(t *testing.T) {
	t.Parallel()
	m := newMockS3("test-bucket")
	defer m.close()
	b := newTestBackend(t, m)
	defer b.Close()

	w := &recordingWriterAt{want: nil}
	err := b.GetConcurrent(context.Background(), "does-not-exist.bin", w, 4)
	if err == nil {
		t.Fatal("expected error for missing key, got nil")
	}
}
