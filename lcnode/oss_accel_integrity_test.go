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

package lcnode

import (
	"testing"
	"time"
)

// The drift discriminator's whole job is deciding whether to FOLLOW a remote
// change or FLAG it. Getting it backwards either silently adopts corrupted
// data or permanently read-blocks legitimately-updated files, so the truth
// table is pinned here rather than only exercised on a live cluster.
func TestClassifyOssAccelMismatch(t *testing.T) {
	flushed := time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)
	flushedRaw := flushed.Format(time.RFC3339)

	cases := []struct {
		name        string
		flushedAt   string
		remoteMtime time.Time
		want        ossAccelMismatchVerdict
	}{
		{
			name:        "remote newer than our flush is an external update",
			flushedAt:   flushedRaw,
			remoteMtime: flushed.Add(time.Minute),
			want:        ossAccelMismatchExternalUpdate,
		},
		{
			name:        "remote older than our flush means nobody rewrote it",
			flushedAt:   flushedRaw,
			remoteMtime: flushed.Add(-time.Minute),
			want:        ossAccelMismatchSuspectCorruption,
		},
		{
			// Equal timestamps mean the object we are looking at is the one we
			// wrote, so a content mismatch is NOT explained by someone else
			// writing — must not be treated as an update.
			name:        "remote equal to our flush is not an update",
			flushedAt:   flushedRaw,
			remoteMtime: flushed,
			want:        ossAccelMismatchSuspectCorruption,
		},
		{
			name:        "missing flushedAt is undecidable",
			flushedAt:   "",
			remoteMtime: flushed.Add(time.Minute),
			want:        ossAccelMismatchUninterpretable,
		},
		{
			name:        "unparsable flushedAt is as uninformative as missing",
			flushedAt:   "not-a-timestamp",
			remoteMtime: flushed.Add(time.Minute),
			want:        ossAccelMismatchUninterpretable,
		},
		{
			// A backend that gave us no mtime cannot support the comparison in
			// either direction — must not default to "update".
			name:        "zero remote mtime is undecidable",
			flushedAt:   flushedRaw,
			remoteMtime: time.Time{},
			want:        ossAccelMismatchUninterpretable,
		},
		{
			name:        "both missing is undecidable",
			flushedAt:   "",
			remoteMtime: time.Time{},
			want:        ossAccelMismatchUninterpretable,
		},
	}
	for _, c := range cases {
		if got := classifyOssAccelMismatch(c.flushedAt, c.remoteMtime); got != c.want {
			t.Errorf("%s: classifyOssAccelMismatch(%q, %v) = %v, want %v",
				c.name, c.flushedAt, c.remoteMtime, got, c.want)
		}
	}
}

// The default verdict must be the conservative one: any future edit that adds
// a branch and forgets to classify it should flag rather than silently follow.
func TestUninterpretableIsTheZeroVerdict(t *testing.T) {
	var zero ossAccelMismatchVerdict
	if zero != ossAccelMismatchUninterpretable {
		t.Fatal("the zero value of ossAccelMismatchVerdict must be Uninterpretable (fall back to existing behavior), not ExternalUpdate (silently follow remote)")
	}
}

// Conclusive gates whether a mismatch is even considered. An observation that
// carries no remote checksum must never read as a mismatch, or every object
// whose sha256 stamp failed at flush time would be flagged.
func TestOssAccelS3ObservationConclusiveness(t *testing.T) {
	noChecksum := ossAccelS3Observation{Size: 10, Mtime: time.Now()}
	if noChecksum.Conclusive {
		t.Fatal("an observation with no remote checksum must not be Conclusive")
	}

	obs := ossAccelS3Observation{Checksum: "abc", Size: 10, Mtime: time.Now(), Conclusive: true}
	if !obs.Matches("abc") {
		t.Fatal("Matches must be true for an identical checksum")
	}
	if obs.Matches("def") {
		t.Fatal("Matches must be false for a differing checksum")
	}
}
