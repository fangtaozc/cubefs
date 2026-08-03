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

	"github.com/cubefs/cubefs/proto"
)

func TestOssAccelIsFlushCandidate(t *testing.T) {
	now := time.Now()
	const minSizeBytes = uint64(1024)
	const idleThreshold = time.Hour

	baseInfo := func() *proto.InodeInfo {
		return &proto.InodeInfo{
			StorageClass: proto.StorageClass_Replica_HDD,
			Size:         2048,
			ModifyTime:   now.Add(-2 * time.Hour),
		}
	}

	cases := []struct {
		name   string
		xattrs map[string]string
		info   *proto.InodeInfo
		want   ossAccelFlushCandidateVerdict
	}{
		{
			name:   "pinned is not eligible",
			xattrs: map[string]string{proto.XAttrKeyOSSAccelPin: "true"},
			info:   baseInfo(),
			want:   ossAccelFlushCandidateNotEligible,
		},
		{
			name:   "already has s3key is not eligible",
			xattrs: map[string]string{proto.XAttrKeyOSSAccelS3Key: "some/key"},
			info:   baseInfo(),
			want:   ossAccelFlushCandidateNotEligible,
		},
		{
			name:   "non-replica storage class is not eligible",
			xattrs: map[string]string{},
			info: &proto.InodeInfo{
				StorageClass: proto.StorageClass_BlobStore,
				Size:         2048,
				ModifyTime:   now.Add(-2 * time.Hour),
			},
			want: ossAccelFlushCandidateNotEligible,
		},
		{
			name:   "too small is a near-miss",
			xattrs: map[string]string{},
			info: &proto.InodeInfo{
				StorageClass: proto.StorageClass_Replica_HDD,
				Size:         512,
				ModifyTime:   now.Add(-2 * time.Hour),
			},
			want: ossAccelFlushCandidateTooYoungOrSmall,
		},
		{
			name:   "not idle long enough is a near-miss",
			xattrs: map[string]string{},
			info: &proto.InodeInfo{
				StorageClass: proto.StorageClass_Replica_HDD,
				Size:         2048,
				ModifyTime:   now.Add(-10 * time.Minute),
			},
			want: ossAccelFlushCandidateTooYoungOrSmall,
		},
		{
			name:   "qualifies on every axis",
			xattrs: map[string]string{},
			info:   baseInfo(),
			want:   ossAccelFlushCandidateMatch,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ossAccelIsFlushCandidate(tc.xattrs, tc.info, now, minSizeBytes, idleThreshold)
			if got != tc.want {
				t.Errorf("ossAccelIsFlushCandidate() = %v, want %v", got, tc.want)
			}
		})
	}
}
