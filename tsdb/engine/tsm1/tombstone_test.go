package tsm1_test

import (
	"bytes"
	"os"
	"testing"

	"github.com/influxdata/influxdb/v2/tsdb/engine/tsm1"
	"github.com/stretchr/testify/require"
)

func TestTombstoner_Add(t *testing.T) {
	dir := t.TempDir()

	f := MustTempFile(t, dir)
	ts := tsm1.NewTombstoner(f.Name(), nil)

	entries := mustReadAll(ts)
	if got, exp := len(entries), 0; got != exp {
		t.Fatalf("length mismatch: got %v, exp %v", got, exp)
	}

	stats := ts.TombstoneStats()
	require.False(t, stats.TombstoneExists)

	ts.Add([][]byte{[]byte("foo")})

	if err := ts.Flush(); err != nil {
		t.Fatalf("unexpected error flushing tombstone: %v", err)
	}

	entries = mustReadAll(ts)
	stats = ts.TombstoneStats()
	require.True(t, stats.TombstoneExists)
	require.NotZero(t, stats.Size)
	require.NotZero(t, stats.LastModified)
	require.NotEmpty(t, stats.Path)

	if got, exp := len(entries), 1; got != exp {
		t.Fatalf("length mismatch: got %v, exp %v", got, exp)
	}

	if got, exp := string(entries[0].Key), "foo"; got != exp {
		t.Fatalf("value mismatch: got %v, exp %v", got, exp)
	}

	// Use a new Tombstoner to verify values are persisted
	ts = tsm1.NewTombstoner(f.Name(), nil)
	entries = mustReadAll(ts)
	if got, exp := len(entries), 1; got != exp {
		t.Fatalf("length mismatch: got %v, exp %v", got, exp)
	}

	if got, exp := string(entries[0].Key), "foo"; got != exp {
		t.Fatalf("value mismatch: got %v, exp %v", got, exp)
	}
}

func TestTombstoner_Add_LargeKey(t *testing.T) {
	dir := t.TempDir()

	f := MustTempFile(t, dir)
	ts := tsm1.NewTombstoner(f.Name(), nil)

	entries := mustReadAll(ts)
	if got, exp := len(entries), 0; got != exp {
		t.Fatalf("length mismatch: got %v, exp %v", got, exp)
	}

	stats := ts.TombstoneStats()
	require.False(t, stats.TombstoneExists)

	key := bytes.Repeat([]byte{'a'}, 4096)
	ts.Add([][]byte{key})

	if err := ts.Flush(); err != nil {
		t.Fatalf("unexpected error flushing tombstone: %v", err)
	}

	entries = mustReadAll(ts)
	stats = ts.TombstoneStats()
	require.True(t, stats.TombstoneExists)
	require.NotZero(t, stats.Size)
	require.NotZero(t, stats.LastModified)
	require.NotEmpty(t, stats.Path)

	if got, exp := len(entries), 1; got != exp {
		t.Fatalf("length mismatch: got %v, exp %v", got, exp)
	}

	if got, exp := string(entries[0].Key), string(key); got != exp {
		t.Fatalf("value mismatch: got %v, exp %v", got, exp)
	}

	// Use a new Tombstoner to verify values are persisted
	ts = tsm1.NewTombstoner(f.Name(), nil)
	entries = mustReadAll(ts)
	if got, exp := len(entries), 1; got != exp {
		t.Fatalf("length mismatch: got %v, exp %v", got, exp)
	}

	if got, exp := string(entries[0].Key), string(key); got != exp {
		t.Fatalf("value mismatch: got %v, exp %v", got, exp)
	}
}

func TestTombstoner_Add_Multiple(t *testing.T) {
	dir := t.TempDir()

	f := MustTempFile(t, dir)
	ts := tsm1.NewTombstoner(f.Name(), nil)

	entries := mustReadAll(ts)
	if got, exp := len(entries), 0; got != exp {
		t.Fatalf("length mismatch: got %v, exp %v", got, exp)
	}

	stats := ts.TombstoneStats()
	require.False(t, stats.TombstoneExists)

	ts.Add([][]byte{[]byte("foo")})

	if err := ts.Flush(); err != nil {
		t.Fatalf("unexpected error flushing tombstone: %v", err)
	}

	ts.Add([][]byte{[]byte("bar")})

	if err := ts.Flush(); err != nil {
		t.Fatalf("unexpected error flushing tombstone: %v", err)
	}

	entries = mustReadAll(ts)
	stats = ts.TombstoneStats()
	require.True(t, stats.TombstoneExists)
	require.NotZero(t, stats.Size)
	require.NotZero(t, stats.LastModified)
	require.NotEmpty(t, stats.Path)

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
	ts = tsm1.NewTombstoner(f.Name(), nil)
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
	dir := t.TempDir()

	f := MustTempFile(t, dir)
	ts := tsm1.NewTombstoner(f.Name(), nil)

	entries := mustReadAll(ts)
	if got, exp := len(entries), 0; got != exp {
		t.Fatalf("length mismatch: got %v, exp %v", got, exp)
	}

	ts.Add([][]byte{})

	if err := ts.Flush(); err != nil {
		t.Fatalf("unexpected error flushing tombstone: %v", err)
	}

	// Use a new Tombstoner to verify values are persisted
	ts = tsm1.NewTombstoner(f.Name(), nil)
	entries = mustReadAll(ts)
	if got, exp := len(entries), 0; got != exp {
		t.Fatalf("length mismatch: got %v, exp %v", got, exp)
	}

	stats := ts.TombstoneStats()
	require.False(t, stats.TombstoneExists)
}

func TestTombstoner_Delete(t *testing.T) {
	dir := t.TempDir()

	f := MustTempFile(t, dir)
	ts := tsm1.NewTombstoner(f.Name(), nil)

	ts.Add([][]byte{[]byte("foo")})

	if err := ts.Flush(); err != nil {
		t.Fatalf("unexpected error flushing: %v", err)
	}

	// Use a new Tombstoner to verify values are persisted
	ts = tsm1.NewTombstoner(f.Name(), nil)
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

	stats := ts.TombstoneStats()
	require.False(t, stats.TombstoneExists)

	ts = tsm1.NewTombstoner(f.Name(), nil)
	entries = mustReadAll(ts)
	if got, exp := len(entries), 0; got != exp {
		t.Fatalf("length mismatch: got %v, exp %v", got, exp)
	}
}

func TestTombstoner_ReadV1(t *testing.T) {
	dir := t.TempDir()

	f := MustTempFile(t, dir)
	if err := os.WriteFile(f.Name(), []byte("foo\n"), 0x0600); err != nil {
		t.Fatalf("write v1 file: %v", err)
	}
	f.Close()

	if err := os.Rename(f.Name(), f.Name()+"."+tsm1.TombstoneFileExtension); err != nil {
		t.Fatalf("rename tombstone failed: %v", err)
	}

	ts := tsm1.NewTombstoner(f.Name(), nil)

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
	ts = tsm1.NewTombstoner(f.Name(), nil)
	entries = mustReadAll(ts)
	if got, exp := len(entries), 1; got != exp {
		t.Fatalf("length mismatch: got %v, exp %v", got, exp)
	}

	if got, exp := string(entries[0].Key), "foo"; got != exp {
		t.Fatalf("value mismatch: got %v, exp %v", got, exp)
	}
}

func TestTombstoner_ReadEmptyV1(t *testing.T) {
	dir := t.TempDir()

	f := MustTempFile(t, dir)
	f.Close()
	if err := os.Rename(f.Name(), f.Name()+"."+tsm1.TombstoneFileExtension); err != nil {
		t.Fatalf("rename tombstone failed: %v", err)
	}

	ts := tsm1.NewTombstoner(f.Name(), nil)

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
