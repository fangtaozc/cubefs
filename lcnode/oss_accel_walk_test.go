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

	"github.com/cubefs/cubefs/proto"
)

func dentries(pairs ...any) []proto.Dentry {
	out := make([]proto.Dentry, 0, len(pairs)/2)
	for i := 0; i < len(pairs); i += 2 {
		out = append(out, proto.Dentry{Name: pairs[i].(string), Inode: uint64(pairs[i+1].(int))})
	}
	return out
}

// The batch APIs return results unordered. Matching by slice index instead of
// by inode would attach one file's metadata to a different file — and for these
// sweeps that means acting destructively on the wrong file. This is the single
// most important property in this file.
func TestAssembleOssAccelWalkPageMatchesByInodeNotIndex(t *testing.T) {
	files := dentries("a.bin", 10, "b.bin", 20, "c.bin", 30)
	// Deliberately reversed / shuffled relative to files.
	infos := []*proto.InodeInfo{
		{Inode: 30, Size: 300},
		{Inode: 10, Size: 100},
		{Inode: 20, Size: 200},
	}
	xattrs := []*proto.XAttrInfo{
		{Inode: 20, XAttrs: map[string]string{proto.XAttrKeyOSSAccelS3Key: "b"}},
		{Inode: 30, XAttrs: map[string]string{proto.XAttrKeyOSSAccelS3Key: "c"}},
		{Inode: 10, XAttrs: map[string]string{proto.XAttrKeyOSSAccelS3Key: "a"}},
	}

	got := assembleOssAccelWalkPage(files, infos, xattrs)
	if len(got) != 3 {
		t.Fatalf("want 3 entries, got %d", len(got))
	}
	want := []struct {
		name  string
		ino   uint64
		size  uint64
		s3key string
	}{
		{"a.bin", 10, 100, "a"},
		{"b.bin", 20, 200, "b"},
		{"c.bin", 30, 300, "c"},
	}
	for i, w := range want {
		if got[i].dentry.Name != w.name {
			t.Errorf("entry %d: order not preserved, name = %q want %q", i, got[i].dentry.Name, w.name)
		}
		if got[i].info.Inode != w.ino || got[i].info.Size != w.size {
			t.Errorf("entry %d (%s): wrong inode attached: got ino=%d size=%d want ino=%d size=%d",
				i, w.name, got[i].info.Inode, got[i].info.Size, w.ino, w.size)
		}
		if got[i].attrs[proto.XAttrKeyOSSAccelS3Key] != w.s3key {
			t.Errorf("entry %d (%s): wrong xattrs attached: s3key = %q want %q",
				i, w.name, got[i].attrs[proto.XAttrKeyOSSAccelS3Key], w.s3key)
		}
	}
}

// A file with no xattrs at all is simply absent from BatchGetXAttr's result
// (metanode only emits entries for inodes with an extendTree record). It must
// still be visited, with an empty non-nil map — visitors index the map directly.
func TestAssembleOssAccelWalkPageMissingXattrsYieldEmptyMap(t *testing.T) {
	files := dentries("managed.bin", 10, "plain.txt", 20)
	infos := []*proto.InodeInfo{{Inode: 10}, {Inode: 20}}
	xattrs := []*proto.XAttrInfo{
		{Inode: 10, XAttrs: map[string]string{proto.XAttrKeyOSSAccelS3Key: "managed.bin"}},
		// inode 20 absent: it has no xattrs.
	}

	got := assembleOssAccelWalkPage(files, infos, xattrs)
	if len(got) != 2 {
		t.Fatalf("a file with no xattrs must still be visited: want 2 entries, got %d", len(got))
	}
	if got[1].attrs == nil {
		t.Fatal("attrs must be an empty map, never nil — visitors index it directly")
	}
	if len(got[1].attrs) != 0 {
		t.Fatalf("want empty attrs for the unmanaged file, got %v", got[1].attrs)
	}
	if got[1].attrs[proto.XAttrKeyOSSAccelS3Key] != "" {
		t.Fatal("reading a missing key from the empty map must yield the zero value, not panic or leak another file's value")
	}
}

// Preserves the pre-batching behavior at oss_accel_walk.go's old per-file
// InodeGet_ll: an inode that vanished between the readdir and the fetch is
// skipped, not surfaced as an error and not visited with a nil info.
func TestAssembleOssAccelWalkPageDropsRacedAwayInodes(t *testing.T) {
	files := dentries("survives.bin", 10, "deleted.bin", 20, "also-survives.bin", 30)
	infos := []*proto.InodeInfo{{Inode: 10}, {Inode: 30}} // 20 raced away
	xattrs := []*proto.XAttrInfo{{Inode: 20, XAttrs: map[string]string{"stale": "1"}}}

	got := assembleOssAccelWalkPage(files, infos, xattrs)
	if len(got) != 2 {
		t.Fatalf("want the deleted file dropped: 2 entries, got %d", len(got))
	}
	for _, e := range got {
		if e.dentry.Name == "deleted.bin" {
			t.Fatal("a file whose inode is gone must not be visited")
		}
		if e.info == nil {
			t.Fatal("info must never be nil for a visited entry")
		}
	}
	if got[0].dentry.Name != "survives.bin" || got[1].dentry.Name != "also-survives.bin" {
		t.Fatalf("order not preserved after a drop: got %q,%q", got[0].dentry.Name, got[1].dentry.Name)
	}
}

// A whole-page BatchGetXAttr failure degrades to "no xattrs for this page"
// (fetchOssAccelWalkPage passes nil) rather than dropping the files entirely —
// the sweeps' own filters then skip them for lack of an s3key.
func TestAssembleOssAccelWalkPageNilXattrs(t *testing.T) {
	files := dentries("a.bin", 10, "b.bin", 20)
	infos := []*proto.InodeInfo{{Inode: 10}, {Inode: 20}}

	got := assembleOssAccelWalkPage(files, infos, nil)
	if len(got) != 2 {
		t.Fatalf("want both files still visited, got %d", len(got))
	}
	for i, e := range got {
		if e.attrs == nil || len(e.attrs) != 0 {
			t.Errorf("entry %d: want empty non-nil attrs, got %v", i, e.attrs)
		}
	}
}

func TestAssembleOssAccelWalkPageEmptyAndNilInputs(t *testing.T) {
	if got := assembleOssAccelWalkPage(nil, nil, nil); len(got) != 0 {
		t.Fatalf("nil input must yield no entries, got %d", len(got))
	}
	// Every inode missing (e.g. BatchInodeGet returned nothing) => no entries,
	// not a panic.
	if got := assembleOssAccelWalkPage(dentries("a", 1), nil, nil); len(got) != 0 {
		t.Fatalf("all-missing inodes must yield no entries, got %d", len(got))
	}
}

// Defensive: nil elements inside the batch slices must not panic. Neither API
// documents that it never emits one.
func TestAssembleOssAccelWalkPageTolerateNilElements(t *testing.T) {
	files := dentries("a.bin", 10)
	infos := []*proto.InodeInfo{nil, {Inode: 10}}
	xattrs := []*proto.XAttrInfo{nil, {Inode: 10, XAttrs: map[string]string{"k": "v"}}}

	got := assembleOssAccelWalkPage(files, infos, xattrs)
	if len(got) != 1 {
		t.Fatalf("want 1 entry, got %d", len(got))
	}
	if got[0].attrs["k"] != "v" {
		t.Fatalf("want xattrs attached despite a nil element, got %v", got[0].attrs)
	}
}

// Duplicate inodes in a page (hard links to the same inode under different
// names) must each get the metadata, not just the first.
func TestAssembleOssAccelWalkPageHardLinks(t *testing.T) {
	files := dentries("link-a", 10, "link-b", 10)
	infos := []*proto.InodeInfo{{Inode: 10, Size: 7}}
	xattrs := []*proto.XAttrInfo{{Inode: 10, XAttrs: map[string]string{proto.XAttrKeyOSSAccelS3Key: "shared"}}}

	got := assembleOssAccelWalkPage(files, infos, xattrs)
	if len(got) != 2 {
		t.Fatalf("both hard links must be visited, got %d", len(got))
	}
	for i, e := range got {
		if e.info.Size != 7 || e.attrs[proto.XAttrKeyOSSAccelS3Key] != "shared" {
			t.Errorf("entry %d: hard link lost its metadata: info=%v attrs=%v", i, e.info, e.attrs)
		}
	}
}
