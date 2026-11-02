package tsm1_test

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
	"github.com/stretchr/testify/require"
)

func TestTombstoner_Add(t *testing.T) {
	dir := MustTempDir()
	defer func() { os.RemoveAll(dir) }()

	f := MustTempFile(dir)
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
	dir := MustTempDir()
	defer func() { os.RemoveAll(dir) }()

	f := MustTempFile(dir)
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
	dir := MustTempDir()
	defer func() { os.RemoveAll(dir) }()

	f := MustTempFile(dir)
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

func TestTombstoner_RollbackAfterCommit(t *testing.T) {
	f := MustTempFile(t.TempDir())
	defer f.Close()

	ts := tsm1.NewTombstoner(f.Name(), nil)
	require.NoError(t, ts.Add([][]byte{[]byte("first")}))
	require.NoError(t, ts.Flush())

	require.NoError(t, ts.Add([][]byte{[]byte("rolled-back")}))
	require.NoError(t, ts.Rollback())

	require.NoError(t, ts.Add([][]byte{[]byte("last")}))
	require.NoError(t, ts.Flush())

	entries := mustReadAll(tsm1.NewTombstoner(f.Name(), nil))
	require.Len(t, entries, 2)
	require.Equal(t, "first", string(entries[0].Key))
	require.Equal(t, "last", string(entries[1].Key))
}

// dirNames returns the sorted file names in dir; used to prove a failed commit
// leaves no temp file behind.
func dirNames(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

func TestTombstoner_FlushFailureRetry(t *testing.T) {
	dir := t.TempDir()
	f := MustTempFile(dir)
	defer f.Close()
	before := dirNames(t, dir)

	ts := tsm1.NewTombstoner(f.Name(), nil)
	writeErr := errors.New("observer rejected tombstone")
	ts.WithObserver(mockObserver{
		fileFinishing: func(string) error { return writeErr },
		fileUnlinking: func(string) error { return nil },
	})
	require.NoError(t, ts.Add([][]byte{[]byte("failed")}))
	require.ErrorIs(t, ts.Flush(), writeErr)
	require.Equal(t, before, dirNames(t, dir), "failed commit must remove its temp file")

	ts.WithObserver(mockObserver{
		fileFinishing: func(string) error { return nil },
		fileUnlinking: func(string) error { return nil },
	})
	require.NoError(t, ts.Add([][]byte{[]byte("committed")}))
	require.NoError(t, ts.Flush())

	entries := mustReadAll(tsm1.NewTombstoner(f.Name(), nil))
	require.Len(t, entries, 1)
	require.Equal(t, "committed", string(entries[0].Key))
}

func TestTombstoner_V3CommitFailureRetry(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "0001.tsm")
	var existing bytes.Buffer
	existing.Write([]byte{0, 0, 0x15, 0x03})
	gz := gzip.NewWriter(&existing)
	require.NoError(t, gz.Close())
	require.NoError(t, os.WriteFile(filepath.Join(dir, "0001.tombstone"), existing.Bytes(), 0o600))

	before := dirNames(t, dir)
	ts := tsm1.NewTombstoner(path, nil)
	writeErr := errors.New("observer rejected v3 tombstone")
	ts.WithObserver(mockObserver{
		fileFinishing: func(string) error { return writeErr },
		fileUnlinking: func(string) error { return nil },
	})
	require.ErrorIs(t, ts.Add([][]byte{[]byte("failed")}), writeErr)
	require.Equal(t, before, dirNames(t, dir), "failed v3 commit must remove its temp file")

	ts.WithObserver(mockObserver{
		fileFinishing: func(string) error { return nil },
		fileUnlinking: func(string) error { return nil },
	})
	require.NoError(t, ts.Add([][]byte{[]byte("committed")}))

	entries := mustReadAll(tsm1.NewTombstoner(path, nil))
	require.Len(t, entries, 1)
	require.Equal(t, "committed", string(entries[0].Key))
}

func TestTombstoner_Add_Empty(t *testing.T) {
	dir := MustTempDir()
	defer func() { os.RemoveAll(dir) }()

	f := MustTempFile(dir)
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
	dir := MustTempDir()
	defer func() { os.RemoveAll(dir) }()

	f := MustTempFile(dir)
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
	dir := MustTempDir()
	defer func() { os.RemoveAll(dir) }()

	f := MustTempFile(dir)
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
	dir := MustTempDir()
	defer func() { os.RemoveAll(dir) }()

	f := MustTempFile(dir)
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

// BenchmarkTombstoner_Flush performs one AddRange+Flush per iteration,
// rotating across a set of tombstone files. Using more files than the gzip
// writer pool holds exercises the overflow path.
func BenchmarkTombstoner_Flush(b *testing.B) {
	for _, files := range []int{1, 8, 64} {
		b.Run(fmt.Sprintf("files=%d", files), func(b *testing.B) {
			dir := b.TempDir()
			tombstoners := make([]*tsm1.Tombstoner, files)
			for i := range tombstoners {
				tombstoners[i] = tsm1.NewTombstoner(filepath.Join(dir, fmt.Sprintf("%05d.tsm", i)), nil)
			}
			key := [][]byte{[]byte("cpu,host=server-01,region=us-west#!~#value")}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ts := tombstoners[i%files]
				if err := ts.AddRange(key, int64(i), int64(i)); err != nil {
					b.Fatal(err)
				}
				if err := ts.Flush(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
