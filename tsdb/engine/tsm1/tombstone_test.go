package tsm1_test

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

func TestTombstoner_Add(t *testing.T) {
	dir := MustTempDir()
	defer func() { os.RemoveAll(dir) }()

	f := MustTempFile(dir)
	ts := &tsm1.Tombstoner{Path: f.Name()}

	entries := mustReadAll(ts)
	if got, exp := len(entries), 0; got != exp {
		t.Fatalf("length mismatch: got %v, exp %v", got, exp)
	}

	stats := ts.TombstoneFiles()
	if got, exp := len(stats), 0; got != exp {
		t.Fatalf("stat length mismatch: got %v, exp %v", got, exp)
	}

	ts.Add([][]byte{[]byte("foo")})

	if err := ts.Flush(); err != nil {
		t.Fatalf("unexpected error flushing tombstone: %v", err)
	}

	entries = mustReadAll(ts)
	stats = ts.TombstoneFiles()
	if got, exp := len(stats), 1; got != exp {
		t.Fatalf("stat length mismatch: got %v, exp %v", got, exp)
	}

	if stats[0].Size == 0 {
		t.Fatalf("got size %v, exp > 0", stats[0].Size)
	}

	if stats[0].LastModified == 0 {
		t.Fatalf("got lastModified %v, exp > 0", stats[0].LastModified)
	}

	if stats[0].Path == "" {
		t.Fatalf("got path %v, exp != ''", stats[0].Path)
	}

	if got, exp := len(entries), 1; got != exp {
		t.Fatalf("length mismatch: got %v, exp %v", got, exp)
	}

	if got, exp := string(entries[0].Key), "foo"; got != exp {
		t.Fatalf("value mismatch: got %v, exp %v", got, exp)
	}

	// Use a new Tombstoner to verify values are persisted
	ts = &tsm1.Tombstoner{Path: f.Name()}
	entries = mustReadAll(ts)
	if got, exp := len(entries), 1; got != exp {
		t.Fatalf("length mismatch: got %v, exp %v", got, exp)
	}

	if got, exp := string(entries[0].Key), "foo"; got != exp {
		t.Fatalf("value mismatch: got %v, exp %v", got, exp)
	}
}

func TestTombstoner_Add_Multiple(t *testing.T) {
	dir := MustTempDir()
	defer func() { os.RemoveAll(dir) }()

	f := MustTempFile(dir)
	ts := &tsm1.Tombstoner{Path: f.Name()}

	entries := mustReadAll(ts)
	if got, exp := len(entries), 0; got != exp {
		t.Fatalf("length mismatch: got %v, exp %v", got, exp)
	}

	stats := ts.TombstoneFiles()
	if got, exp := len(stats), 0; got != exp {
		t.Fatalf("stat length mismatch: got %v, exp %v", got, exp)
	}

	ts.Add([][]byte{[]byte("foo")})

	if err := ts.Flush(); err != nil {
		t.Fatalf("unexpected error flushing tombstone: %v", err)
	}

	ts.Add([][]byte{[]byte("bar")})

	if err := ts.Flush(); err != nil {
		t.Fatalf("unexpected error flushing tombstone: %v", err)
	}

	entries = mustReadAll(ts)
	stats = ts.TombstoneFiles()
	if got, exp := len(stats), 1; got != exp {
		t.Fatalf("stat length mismatch: got %v, exp %v", got, exp)
	}

	if stats[0].Size == 0 {
		t.Fatalf("got size %v, exp > 0", stats[0].Size)
	}

	if stats[0].LastModified == 0 {
		t.Fatalf("got lastModified %v, exp > 0", stats[0].LastModified)
	}

	if stats[0].Path == "" {
		t.Fatalf("got path %v, exp != ''", stats[0].Path)
	}

	if got, exp := len(entries), 2; got != exp {
		t.Fatalf("length mismatch: got %v, exp %v", got, exp)
	}

	if got, exp := string(entries[0].Key), "foo"; got != exp {
		t.Fatalf("value mismatch: got %v, exp %v", got, exp)
	}

	if got, exp := string(entries[1].Key), "bar"; got != exp {
		t.Fatalf("value mismatch: got %v, exp %v", got, exp)
	}

	// Use a new Tombstoner to verify values are persisted
	ts = &tsm1.Tombstoner{Path: f.Name()}
	entries = mustReadAll(ts)
	if got, exp := len(entries), 2; got != exp {
		t.Fatalf("length mismatch: got %v, exp %v", got, exp)
	}

	if got, exp := string(entries[0].Key), "foo"; got != exp {
		t.Fatalf("value mismatch: got %v, exp %v", got, exp)
	}

	if got, exp := string(entries[1].Key), "bar"; got != exp {
		t.Fatalf("value mismatch: got %v, exp %v", got, exp)
	}

}

func TestTombstoner_Add_Empty(t *testing.T) {
	dir := MustTempDir()
	defer func() { os.RemoveAll(dir) }()

	f := MustTempFile(dir)
	ts := &tsm1.Tombstoner{Path: f.Name()}

	entries := mustReadAll(ts)
	if got, exp := len(entries), 0; got != exp {
		t.Fatalf("length mismatch: got %v, exp %v", got, exp)
	}

	ts.Add([][]byte{})

	if err := ts.Flush(); err != nil {
		t.Fatalf("unexpected error flushing tombstone: %v", err)
	}

	// Use a new Tombstoner to verify values are persisted
	ts = &tsm1.Tombstoner{Path: f.Name()}
	entries = mustReadAll(ts)
	if got, exp := len(entries), 0; got != exp {
		t.Fatalf("length mismatch: got %v, exp %v", got, exp)
	}

	stats := ts.TombstoneFiles()
	if got, exp := len(stats), 0; got != exp {
		t.Fatalf("stat length mismatch: got %v, exp %v", got, exp)
	}

}

func TestTombstoner_Delete(t *testing.T) {
	dir := MustTempDir()
	defer func() { os.RemoveAll(dir) }()

	f := MustTempFile(dir)
	ts := &tsm1.Tombstoner{Path: f.Name()}

	ts.Add([][]byte{[]byte("foo")})

	if err := ts.Flush(); err != nil {
		t.Fatalf("unexpected error flushing: %v", err)
	}

	// Use a new Tombstoner to verify values are persisted
	ts = &tsm1.Tombstoner{Path: f.Name()}
	entries := mustReadAll(ts)
	if got, exp := len(entries), 1; got != exp {
		t.Fatalf("length mismatch: got %v, exp %v", got, exp)
	}

	if got, exp := string(entries[0].Key), "foo"; got != exp {
		t.Fatalf("value mismatch: got %s, exp %s", got, exp)
	}

	if err := ts.Delete(); err != nil {
		fatal(t, "delete tombstone", err)
	}

	stats := ts.TombstoneFiles()
	if got, exp := len(stats), 0; got != exp {
		t.Fatalf("stat length mismatch: got %v, exp %v", got, exp)
	}

	ts = &tsm1.Tombstoner{Path: f.Name()}
	entries = mustReadAll(ts)
	if got, exp := len(entries), 0; got != exp {
		t.Fatalf("length mismatch: got %v, exp %v", got, exp)
	}
}

func TestTombstoner_ReadV1(t *testing.T) {
	dir := MustTempDir()
	defer func() { os.RemoveAll(dir) }()

	f := MustTempFile(dir)
	if err := ioutil.WriteFile(f.Name(), []byte("foo\n"), 0x0600); err != nil {
		t.Fatalf("write v1 file: %v", err)
	}
	f.Close()

	if err := os.Rename(f.Name(), f.Name()+".tombstone"); err != nil {
		t.Fatalf("rename tombstone failed: %v", err)
	}

	ts := &tsm1.Tombstoner{Path: f.Name()}

	// Read once
	_ = mustReadAll(ts)

	// Read again
	entries := mustReadAll(ts)

	if got, exp := len(entries), 1; got != exp {
		t.Fatalf("length mismatch: got %v, exp %v", got, exp)
	}

	if got, exp := string(entries[0].Key), "foo"; got != exp {
		t.Fatalf("value mismatch: got %v, exp %v", got, exp)
	}

	// Use a new Tombstoner to verify values are persisted
	ts = &tsm1.Tombstoner{Path: f.Name()}
	entries = mustReadAll(ts)
	if got, exp := len(entries), 1; got != exp {
		t.Fatalf("length mismatch: got %v, exp %v", got, exp)
	}

	if got, exp := string(entries[0].Key), "foo"; got != exp {
		t.Fatalf("value mismatch: got %v, exp %v", got, exp)
	}
}

func TestTombstoner_ReadEmptyV1(t *testing.T) {
	dir := MustTempDir()
	defer func() { os.RemoveAll(dir) }()

	f := MustTempFile(dir)
	f.Close()

	if err := os.Rename(f.Name(), f.Name()+".tombstone"); err != nil {
		t.Fatalf("rename tombstone failed: %v", err)
	}

	ts := &tsm1.Tombstoner{Path: f.Name()}

	_ = mustReadAll(ts)

	entries := mustReadAll(ts)
	if got, exp := len(entries), 0; got != exp {
		t.Fatalf("length mismatch: got %v, exp %v", got, exp)
	}
}

func mustReadAll(t *tsm1.Tombstoner) []tsm1.Tombstone {
	var tombstones []tsm1.Tombstone
	if err := t.Walk(func(t tsm1.Tombstone) error {
		b := make([]byte, len(t.Key))
		copy(b, t.Key)
		tombstones = append(tombstones, tsm1.Tombstone{
			Min: t.Min,
			Max: t.Max,
			Key: b,
		})
		return nil
	}); err != nil {
		panic(err)
	}
	return tombstones
}
