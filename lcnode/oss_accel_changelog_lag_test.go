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

import "testing"

func TestOssAccelChangelogLagBytes(t *testing.T) {
	cases := []struct {
		name          string
		changelogSize uint64
		cursor        uint64
		want          uint64
	}{
		{"normal lag", 1000, 400, 600},
		{"fully caught up", 500, 500, 0},
		{"cursor ahead of size (truncated changelog) clamps to zero", 100, 500, 0},
		{"zero size and zero cursor", 0, 0, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ossAccelChangelogLagBytes(tc.changelogSize, tc.cursor)
			if got != tc.want {
				t.Errorf("ossAccelChangelogLagBytes(%v, %v) = %v, want %v", tc.changelogSize, tc.cursor, got, tc.want)
			}
		})
	}
}
