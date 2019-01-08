package tsm1_test

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/tsdb/tsm1"
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
	ts = tsm1.NewTombstoner(f.Name(), nil)
	entries = mustReadAll(ts)
	if got, exp := len(entries), 1; got != exp {
		t.Fatalf("length mismatch: got %v, exp %v", got, exp)
	}

	if got, exp := string(entries[0].Key), "foo"; got != exp {
		t.Fatalf("value mismatch: got %v, exp %v", got, exp)
	}

	if got, exp := entries[0].Prefix, false; got != exp {
		t.Fatalf("value mismatch: got %v, exp %v", got, exp)
	}
}

func TestTombstoner_AddPrefix(t *testing.T) {
	dir := MustTempDir()
	defer func() { os.RemoveAll(dir) }()

	f := MustTempFile(dir)
	ts := tsm1.NewTombstoner(f.Name(), nil)

	entries := mustReadAll(ts)
	if got, exp := len(entries), 0; got != exp {
		t.Fatalf("length mismatch: got %v, exp %v", got, exp)
	}

	stats := ts.TombstoneFiles()
	if got, exp := len(stats), 0; got != exp {
		t.Fatalf("stat length mismatch: got %v, exp %v", got, exp)
	}

	if err := ts.AddPrefix([]byte("some-prefix")); err != nil {
		t.Fatal(err)
	}

	if err := ts.Flush(); err != nil {
		t.Fatalf("unexpected error flushing tombstone: %v", err)
	}

	exp := tsm1.Tombstone{
		Key:    []byte("some-prefix"),
		Min:    math.MinInt64,
		Max:    math.MaxInt64,
		Prefix: true,
	}

	entries = mustReadAll(ts)
	if got, exp := len(entries), 1; got != exp {
		t.Fatalf("length mismatch: got %v, exp %v", got, exp)
	}

	if got := entries[0]; !reflect.DeepEqual(got, exp) {
		t.Fatalf("unexpected tombstone entry. Got %s, expected %s", got, exp)
	}
}

func TestTombstoner_AddPrefixRange(t *testing.T) {
	dir := MustTempDir()
	defer func() { os.RemoveAll(dir) }()

	f := MustTempFile(dir)
	ts := tsm1.NewTombstoner(f.Name(), nil)

	entries := mustReadAll(ts)
	if got, exp := len(entries), 0; got != exp {
		t.Fatalf("length mismatch: got %v, exp %v", got, exp)
	}

	stats := ts.TombstoneFiles()
	if got, exp := len(stats), 0; got != exp {
		t.Fatalf("stat length mismatch: got %v, exp %v", got, exp)
	}

	if err := ts.AddPrefixRange([]byte("some-prefix"), 20, 30); err != nil {
		t.Fatal(err)
	}

	if err := ts.Flush(); err != nil {
		t.Fatalf("unexpected error flushing tombstone: %v", err)
	}

	exp := tsm1.Tombstone{
		Key:    []byte("some-prefix"),
		Min:    20,
		Max:    30,
		Prefix: true,
	}

	entries = mustReadAll(ts)
	if got, exp := len(entries), 1; got != exp {
		t.Fatalf("length mismatch: got %v, exp %v", got, exp)
	}

	if got := entries[0]; !reflect.DeepEqual(got, exp) {
		t.Fatalf("unexpected tombstone entry. Got %s, expected %s", got, exp)
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

	stats := ts.TombstoneFiles()
	if got, exp := len(stats), 0; got != exp {
		t.Fatalf("stat length mismatch: got %v, exp %v", got, exp)
	}

	key := bytes.Repeat([]byte{'a'}, 4096)
	ts.Add([][]byte{key})

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

func TestTombstoner_Add_KeyTooBig(t *testing.T) {
	dir := MustTempDir()
	defer func() { os.RemoveAll(dir) }()

	f := MustTempFile(dir)
	ts := tsm1.NewTombstoner(f.Name(), nil)

	entries := mustReadAll(ts)
	if got, exp := len(entries), 0; got != exp {
		t.Fatalf("length mismatch: got %v, exp %v", got, exp)
	}

	stats := ts.TombstoneFiles()
	if got, exp := len(stats), 0; got != exp {
		t.Fatalf("stat length mismatch: got %v, exp %v", got, exp)
	}

	key := bytes.Repeat([]byte{'a'}, 0x00ffffff) // This is OK.
	if err := ts.Add([][]byte{key}); err != nil {
		t.Fatal(err)
	}

	if err := ts.Flush(); err != nil {
		t.Fatalf("unexpected error flushing tombstone: %v", err)
	}

	key = append(key, 'a') // This is not
	if err := ts.Add([][]byte{key}); err == nil {
		t.Fatalf("got no error, expected key length error")
	}

	if err := ts.Flush(); err != nil {
		t.Fatalf("unexpected error flushing tombstone: %v", err)
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
	ts = tsm1.NewTombstoner(f.Name(), nil)
	entries = mustReadAll(ts)
	if got, exp := len(entries), 2; got != exp {
		t.Fatalf("length mismatch: got %v, exp %v", got, exp)
	}

	if got, exp := string(entries[0].Key), "foo"; got != exp {
		t.Fatalf("value mismatch: got %v, exp %v", got, exp)
	}

	if got, exp := entries[0].Prefix, false; got != exp {
		t.Fatalf("value mismatch: got %v, exp %v", got, exp)
	}

	if got, exp := string(entries[1].Key), "bar"; got != exp {
		t.Fatalf("value mismatch: got %v, exp %v", got, exp)
	}

	if got, exp := entries[1].Prefix, false; got != exp {
		t.Fatalf("value mismatch: got %v, exp %v", got, exp)
	}
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

	stats := ts.TombstoneFiles()
	if got, exp := len(stats), 0; got != exp {
		t.Fatalf("stat length mismatch: got %v, exp %v", got, exp)
	}

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

	stats := ts.TombstoneFiles()
	if got, exp := len(stats), 0; got != exp {
		t.Fatalf("stat length mismatch: got %v, exp %v", got, exp)
	}

	ts = tsm1.NewTombstoner(f.Name(), nil)
	entries = mustReadAll(ts)
	if got, exp := len(entries), 0; got != exp {
		t.Fatalf("length mismatch: got %v, exp %v", got, exp)
	}
}

func TestTombstoner_Existing(t *testing.T) {
	dir := MustTempDir()
	defer func() { os.RemoveAll(dir) }()

	expMin := time.Date(2018, time.December, 12, 0, 0, 0, 0, time.UTC).UnixNano()
	expMax := time.Date(2018, time.December, 13, 0, 0, 0, 0, time.UTC).UnixNano()

	expKeys := make([]string, 100)
	for i := 0; i < len(expKeys); i++ {
		expKeys[i] = fmt.Sprintf("m0,tag0=value%d", i)
	}

	// base-16 encoded v4 tombstone file of above setup.
	v4Raw := `000015041f8b08000000000000ff84d0ab5103401400c0d30850e90291dc` +
		`ff092a41453098303140739108da4273b999f5ab36a5f4f8717cfe3cbf1f` +
		`5fbecf97afb7e3e17af93ddd523a5c6faf3f0f29dd891345a6281495a251` +
		`748a41312962239efe8fed5217b25b5dc8ae7521bbd785ec6217b29b5dc8` +
		`ae7621bbdb85ec7217e2ddecddecddecddecddecddecddecddecddecddec` +
		`dde2dde2dde2dde2dde2dde2dde2dde2dde2dde2ddeaddeaddeaddeaddea` +
		`ddeaddeaddeaddeaddeadde6dde6dde6dde6dde6dde6dde6dde6dde6dde6` +
		`ddeeddeeddeeddeeddeeddeeddeeddeeddeeddeedde1dde1dde1dde1dde1` +
		`dde1dde1dde1dde1dde1dde9dde9dde9dde9dde9dde9dde9dde9dde9dde9` +
		`ddf06e7837bc1bde0def8677c3bbe1ddf06edcedfe050000ffff34593d01` +
		`a20d0000`
	v4Decoded, err := hex.DecodeString(v4Raw)
	if err != nil {
		panic(err)
	}

	f := MustTempFile(dir)
	if _, err := io.Copy(f, bytes.NewReader(v4Decoded)); err != nil {
		panic(err)
	}
	if err := f.Close(); err != nil {
		panic(err)
	}

	name := f.Name() + ".tombstone"
	if err := os.Rename(f.Name(), name); err != nil {
		panic(err)
	}

	t.Run("read", func(t *testing.T) {
		ts := tsm1.NewTombstoner(name, nil)
		var gotKeys []string
		if err := ts.Walk(func(tombstone tsm1.Tombstone) error {
			gotKeys = append(gotKeys, string(tombstone.Key))
			if got, exp := tombstone.Min, expMin; got != exp {
				t.Fatalf("got max time %d, expected %d", got, exp)
			} else if got, exp := tombstone.Max, expMax; got != exp {
				t.Fatalf("got max time %d, expected %d", got, exp)
			} else if got, exp := tombstone.Prefix, false; got != exp {
				t.Fatalf("got prefix key, expected non-prefix key")
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(gotKeys, expKeys) {
			t.Fatalf("tombstone entries differ:\n%s\n", cmp.Diff(gotKeys, expKeys, nil))
		}
	})

	t.Run("add_prefix", func(t *testing.T) {
		ts := tsm1.NewTombstoner(name, nil)
		if err := ts.AddPrefixRange([]byte("new-prefix"), 10, 20); err != nil {
			t.Fatal(err)
		}

		if err := ts.Flush(); err != nil {
			t.Fatal(err)
		}

		var got tsm1.Tombstone
		if err := ts.Walk(func(tombstone tsm1.Tombstone) error {
			got = tombstone
			return nil
		}); err != nil {
			t.Fatal(err)
		}

		exp := tsm1.Tombstone{
			Key:    []byte("new-prefix"),
			Min:    10,
			Max:    20,
			Prefix: true,
		}

		if !reflect.DeepEqual(got, exp) {
			t.Fatalf("unexpected tombstone entry. Got %s, expected %s", got, exp)
		}
	})
}

func mustReadAll(t *tsm1.Tombstoner) []tsm1.Tombstone {
	var tombstones []tsm1.Tombstone
	if err := t.Walk(func(t tsm1.Tombstone) error {
		b := make([]byte, len(t.Key))
		copy(b, t.Key)
		tombstones = append(tombstones, tsm1.Tombstone{
			Min:    t.Min,
			Max:    t.Max,
			Key:    b,
			Prefix: t.Prefix,
		})
		return nil
	}); err != nil {
		panic(err)
	}
	return tombstones
}
