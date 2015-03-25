package influxdb_test

import (
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

// Ensure a snapshot can be merged so that the newest files from the two snapshots are returned.
func TestSnapshot_Merge(t *testing.T) {
	for i, tt := range []struct {
		s      *influxdb.Snapshot
		other  *influxdb.Snapshot
		result *influxdb.Snapshot
	}{
		// 0. Mixed higher, lower, equal indices.
		{
			s: &influxdb.Snapshot{Files: []influxdb.SnapshotFile{
				{Name: "a", Size: 10, Index: 1},
				{Name: "b", Size: 10, Index: 10}, // keep: same, first
				{Name: "c", Size: 10, Index: 21}, // keep: higher
				{Name: "e", Size: 10, Index: 15}, // keep: higher
			}},
			other: &influxdb.Snapshot{Files: []influxdb.SnapshotFile{
				{Name: "a", Size: 20, Index: 2}, // keep: higher
				{Name: "b", Size: 20, Index: 10},
				{Name: "c", Size: 20, Index: 11},
				{Name: "d", Size: 20, Index: 14}, // keep: new
				{Name: "e", Size: 20, Index: 12},
			}},
			result: &influxdb.Snapshot{Files: []influxdb.SnapshotFile{
				{Name: "a", Size: 20, Index: 2},
				{Name: "b", Size: 10, Index: 10},
				{Name: "c", Size: 10, Index: 21},
				{Name: "d", Size: 20, Index: 14},
				{Name: "e", Size: 10, Index: 15},
			}},
		},
	} {
		result := tt.s.Merge(tt.other)
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

	// Read snapshot from buffer.
	sr := influxdb.NewSnapshotReader(&buf)

	// Read the manifest.
	if ss, err := sr.Snapshot(); err != nil {
		t.Fatalf("unexpected error(snapshot): %s", err)
	} else if !reflect.DeepEqual(sw.Snapshot, ss) {
		t.Fatalf("snapshot mismatch:\n\nexp=%#v\n\ngot=%#v", sw.Snapshot, ss)
	}

	// Next should be the meta file.
	if f, err := sr.Next(); err != nil {
		t.Fatalf("unexpected error(meta): %s", err)
	} else if !reflect.DeepEqual(f, influxdb.SnapshotFile{Name: "meta", Size: 3, Index: 12}) {
		t.Fatalf("file mismatch(meta): %#v", f)
	} else if b := MustReadAll(sr); string(b) != `foo` {
		t.Fatalf("unexpected file(meta): %s", b)
	}

	// Next should be the shard file.
	if f, err := sr.Next(); err != nil {
		t.Fatalf("unexpected error(shards/1): %s", err)
	} else if !reflect.DeepEqual(f, influxdb.SnapshotFile{Name: "shards/1", Size: 5, Index: 15}) {
		t.Fatalf("file mismatch(shards/1): %#v", f)
	} else if b := MustReadAll(sr); string(b) != `55555` {
		t.Fatalf("unexpected file(shards/1): %s", b)
	}

	// Check for end of snapshot.
	if _, err := sr.Next(); err != io.EOF {
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

// Ensure a SnapshotsReader can read from multiple snapshots.
func TestSnapshotsReader(t *testing.T) {
	var sw *influxdb.SnapshotWriter
	bufs := make([]bytes.Buffer, 2)

	// Snapshot #1
	sw = influxdb.NewSnapshotWriter()
	sw.Snapshot.Files = []influxdb.SnapshotFile{
		{Name: "meta", Size: 3, Index: 12},
		{Name: "shards/1", Size: 5, Index: 15},
	}
	sw.FileWriters["meta"] = &bufCloser{Buffer: *bytes.NewBufferString("foo")}
	sw.FileWriters["shards/1"] = &bufCloser{Buffer: *bytes.NewBufferString("55555")}
	if _, err := sw.WriteTo(&bufs[0]); err != nil {
		t.Fatal(err)
	} else if err = sw.Close(); err != nil {
		t.Fatal(err)
	}

	// Snapshot #2
	sw = influxdb.NewSnapshotWriter()
	sw.Snapshot.Files = []influxdb.SnapshotFile{
		{Name: "meta", Size: 3, Index: 20},
		{Name: "shards/2", Size: 6, Index: 30},
	}
	sw.FileWriters["meta"] = &bufCloser{Buffer: *bytes.NewBufferString("bar")}
	sw.FileWriters["shards/2"] = &bufCloser{Buffer: *bytes.NewBufferString("666666")}
	if _, err := sw.WriteTo(&bufs[1]); err != nil {
		t.Fatal(err)
	} else if err = sw.Close(); err != nil {
		t.Fatal(err)
	}

	// Read and merge snapshots.
	ssr := influxdb.NewSnapshotsReader(&bufs[0], &bufs[1])

	// Next should be the second meta file.
	if f, err := ssr.Next(); err != nil {
		t.Fatalf("unexpected error(meta): %s", err)
	} else if !reflect.DeepEqual(f, influxdb.SnapshotFile{Name: "meta", Size: 3, Index: 20}) {
		t.Fatalf("file mismatch(meta): %#v", f)
	} else if b := MustReadAll(ssr); string(b) != `bar` {
		t.Fatalf("unexpected file(meta): %s", b)
	}

	// Next should be shards/1.
	if f, err := ssr.Next(); err != nil {
		t.Fatalf("unexpected error(shards/1): %s", err)
	} else if !reflect.DeepEqual(f, influxdb.SnapshotFile{Name: "shards/1", Size: 5, Index: 15}) {
		t.Fatalf("file mismatch(shards/1): %#v", f)
	} else if b := MustReadAll(ssr); string(b) != `55555` {
		t.Fatalf("unexpected file(shards/1): %s", b)
	}

	// Next should be shards/2.
	if f, err := ssr.Next(); err != nil {
		t.Fatalf("unexpected error(shards/2): %s", err)
	} else if !reflect.DeepEqual(f, influxdb.SnapshotFile{Name: "shards/2", Size: 6, Index: 30}) {
		t.Fatalf("file mismatch(shards/2): %#v", f)
	} else if b := MustReadAll(ssr); string(b) != `666666` {
		t.Fatalf("unexpected file(shards/2): %s", b)
	}

	// Check for end of snapshot.
	if _, err := ssr.Next(); err != io.EOF {
		t.Fatalf("expected EOF: %s", err)
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
