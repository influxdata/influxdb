package influxdb_test

import (
	"archive/tar"
	"bytes"
	"io"
	"io/ioutil"
	"reflect"
	"testing"

	"github.com/influxdb/influxdb"
)

// Ensure a snapshot can be diff'd so that only newer files are retrieved.
func TestSnapshot_Diff(t *testing.T) {
	for i, tt := range []struct {
		s      *influxdb.Snapshot
		other  *influxdb.Snapshot
		result *influxdb.Snapshot
	}{
		// 0. Mixed higher, lower, equal indices.
		{
			s: &influxdb.Snapshot{Files: []influxdb.SnapshotFile{
				{Name: "a", Index: 1},  // remove: lower index
				{Name: "b", Index: 10}, // remove: equal index
				{Name: "c", Index: 21}, // keep: higher index
				{Name: "d", Index: 15}, // keep: higher index
			}},
			other: &influxdb.Snapshot{Files: []influxdb.SnapshotFile{
				{Name: "a", Index: 2},
				{Name: "b", Index: 10},
				{Name: "c", Index: 11},
				{Name: "d", Index: 14},
			}},
			result: &influxdb.Snapshot{Files: []influxdb.SnapshotFile{
				{Name: "c", Index: 21},
				{Name: "d", Index: 15},
			}},
		},

		// 1. Files in other-only should not be added to diff.
		{
			s: &influxdb.Snapshot{Files: []influxdb.SnapshotFile{
				{Name: "a", Index: 2},
			}},
			other: &influxdb.Snapshot{Files: []influxdb.SnapshotFile{
				{Name: "a", Index: 1},
				{Name: "b", Index: 10},
			}},
			result: &influxdb.Snapshot{Files: []influxdb.SnapshotFile{
				{Name: "a", Index: 2},
			}},
		},

		// 2. Files in s-only should be added to diff.
		{
			s: &influxdb.Snapshot{Files: []influxdb.SnapshotFile{
				{Name: "a", Index: 2},
			}},
			other: &influxdb.Snapshot{Files: []influxdb.SnapshotFile{}},
			result: &influxdb.Snapshot{Files: []influxdb.SnapshotFile{
				{Name: "a", Index: 2},
			}},
		},

		// 3. Empty snapshots should return empty diffs.
		{
			s:      &influxdb.Snapshot{Files: []influxdb.SnapshotFile{}},
			other:  &influxdb.Snapshot{Files: []influxdb.SnapshotFile{}},
			result: &influxdb.Snapshot{Files: nil},
		},
	} {
		result := tt.s.Diff(tt.other)
		if !reflect.DeepEqual(tt.result, result) {
			t.Errorf("%d. mismatch:\n\nexp=%#v\n\ngot=%#v", i, tt.result, result)
		}
	}
}

// Ensure a snapshot writer can write a set of files to an archive
func TestSnapshotWriter(t *testing.T) {
	// Create a new writer with a snapshot and file writers.
	sw := influxdb.NewSnapshotWriter()
	sw.Snapshot.Files = []influxdb.SnapshotFile{
		{Name: "meta", Size: 3, Index: 12},
		{Name: "shards/1", Size: 5, Index: 15},
	}
	sw.FileWriters["meta"] = &bufCloser{Buffer: *bytes.NewBufferString("foo")}
	sw.FileWriters["shards/1"] = &bufCloser{Buffer: *bytes.NewBufferString("55555")}

	// Write the snapshot to a buffer.
	var buf bytes.Buffer
	if _, err := sw.WriteTo(&buf); err != nil {
		t.Fatal(err)
	}

	// Ensure file writers are closed as they're writing.
	if !sw.FileWriters["meta"].(*bufCloser).closed {
		t.Fatal("meta file writer not closed")
	} else if !sw.FileWriters["shards/1"].(*bufCloser).closed {
		t.Fatal("shards/1 file writer not closed")
	}

	// Close writer.
	if err := sw.Close(); err != nil {
		t.Fatal(err)
	}

	// Read archive from buffer.
	tr := tar.NewReader(&buf)

	// First file should be the manifest.
	if hdr, err := tr.Next(); err != nil {
		t.Fatalf("unexpected error(manifest): %s", err)
	} else if hdr.Name != "manifest" {
		t.Fatalf("unexpected header name(manifest): %s", hdr.Name)
	} else if hdr.Size != 87 {
		t.Fatalf("unexpected header size(manifest): %d", hdr.Size)
	} else if b := MustReadAll(tr); string(b) != `{"files":[{"name":"meta","size":3,"index":12},{"name":"shards/1","size":5,"index":15}]}` {
		t.Fatalf("unexpected file(manifest): %s", b)
	}

	// Next should be the meta file.
	if hdr, err := tr.Next(); err != nil {
		t.Fatalf("unexpected error(meta): %s", err)
	} else if hdr.Name != "meta" {
		t.Fatalf("unexpected header name(meta): %s", hdr.Name)
	} else if hdr.Size != 3 {
		t.Fatalf("unexpected header size(meta): %d", hdr.Size)
	} else if b := MustReadAll(tr); string(b) != `foo` {
		t.Fatalf("unexpected file(meta): %s", b)
	}

	// Next should be the shard file.
	if hdr, err := tr.Next(); err != nil {
		t.Fatalf("unexpected error(shards/1): %s", err)
	} else if hdr.Name != "shards/1" {
		t.Fatalf("unexpected header name(shards/1): %s", hdr.Name)
	} else if hdr.Size != 5 {
		t.Fatalf("unexpected header size(shards/1): %d", hdr.Size)
	} else if b := MustReadAll(tr); string(b) != `55555` {
		t.Fatalf("unexpected file(shards/1): %s", b)
	}

	// Check for end of archive.
	if _, err := tr.Next(); err != io.EOF {
		t.Fatalf("expected EOF: %s", err)
	}
}

// Ensure a snapshot writer closes unused file writers.
func TestSnapshotWriter_CloseUnused(t *testing.T) {
	// Create a new writer with a snapshot and file writers.
	sw := influxdb.NewSnapshotWriter()
	sw.Snapshot.Files = []influxdb.SnapshotFile{
		{Name: "meta", Size: 3},
	}
	sw.FileWriters["meta"] = &bufCloser{Buffer: *bytes.NewBufferString("foo")}
	sw.FileWriters["other"] = &bufCloser{Buffer: *bytes.NewBufferString("55555")}

	// Write the snapshot to a buffer.
	var buf bytes.Buffer
	if _, err := sw.WriteTo(&buf); err != nil {
		t.Fatal(err)
	}

	// Ensure other writer is closed.
	// This should happen at the beginning of the write so that it doesn't have
	// to wait until the close of the whole writer.
	if !sw.FileWriters["other"].(*bufCloser).closed {
		t.Fatal("'other' file writer not closed")
	}
}

// bufCloser adds a Close() method to a bytes.Buffer
type bufCloser struct {
	bytes.Buffer
	closed bool
}

// Close marks the buffer as closed.
func (b *bufCloser) Close() error {
	b.closed = true
	return nil
}

// Reads all data from the reader. Panic on error.
func MustReadAll(r io.Reader) []byte {
	b, err := ioutil.ReadAll(r)
	if err != nil {
		panic(err.Error())
	}
	return b
}
