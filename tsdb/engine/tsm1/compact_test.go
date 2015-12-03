package tsm1_test

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/influxdb/influxdb/models"
	"github.com/influxdb/influxdb/tsdb/engine/tsm1"
)

//  Tests compacting a Cache snapshot into a single TSM file
func TestCompactor_Snapshot(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	v1 := tsm1.NewValue(time.Unix(1, 0), float64(1))
	v2 := tsm1.NewValue(time.Unix(1, 0), float64(1))
	v3 := tsm1.NewValue(time.Unix(2, 0), float64(2))

	points1 := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": []tsm1.Value{v1},
		"cpu,host=B#!~#value": []tsm1.Value{v2, v3},
	}

	c := tsm1.NewCache(0)
	for k, v := range points1 {
		if err := c.Write(k, v); err != nil {
			t.Fatalf("failed to write key foo to cache: %s", err.Error())
		}
	}

	compactor := &tsm1.Compactor{
		Dir:         dir,
		FileStore:   &fakeFileStore{},
		MaxFileSize: 1024 << 10,
	}

	files, err := compactor.WriteSnapshot(c)
	if err != nil {
		t.Fatalf("unexpected error writing snapshot: %v", err)
	}

	if got, exp := len(files), 1; got != exp {
		t.Fatalf("files length mismatch: got %v, exp %v", got, exp)
	}

	f, err := os.Open(files[0])
	if err != nil {
		t.Fatalf("unexpected error openting tsm: %v", err)
	}
	r, err := tsm1.NewTSMReader(f)
	if err != nil {
		t.Fatalf("unexpected error creating tsm reader: %v", err)
	}

	keys := r.Keys()
	if got, exp := len(keys), 2; got != exp {
		t.Fatalf("keys length mismatch: got %v, exp %v", got, exp)
	}

	var data = []struct {
		key    string
		points []tsm1.Value
	}{
		{"cpu,host=A#!~#value", []tsm1.Value{v1}},
		{"cpu,host=B#!~#value", []tsm1.Value{v2, v3}},
	}

	for _, p := range data {
		values, err := r.ReadAll(p.key)
		if err != nil {
			t.Fatalf("unexpected error reading: %v", err)
		}

		if got, exp := len(values), len(p.points); got != exp {
			t.Fatalf("values length mismatch: got %v, exp %v", got, exp)
		}

		for i, point := range p.points {
			assertValueEqual(t, values[i], point)
		}
	}
}

// Tests that a single TSM file can be read and iterated over
func TestKeyIterator_TSM_Single(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	v1 := tsm1.NewValue(time.Unix(1, 0), 1.1)
	writes := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": []tsm1.Value{v1},
	}

	r := MustTSMReader(dir, writes)

	iter, err := tsm1.NewTSMKeyIterator(r)
	if err != nil {
		t.Fatalf("unexpected error creating WALKeyIterator: %v", err)
	}

	var readValues bool
	for iter.Next() {
		key, values, err := iter.Read()
		if err != nil {
			t.Fatalf("unexpected error read: %v", err)
		}

		if got, exp := key, "cpu,host=A#!~#value"; got != exp {
			t.Fatalf("key mismatch: got %v, exp %v", got, exp)
		}

		if got, exp := len(values), len(writes); got != exp {
			t.Fatalf("values length mismatch: got %v, exp %v", got, exp)
		}

		for _, v := range values {
			readValues = true
			assertValueEqual(t, v, v1)
		}
	}

	if !readValues {
		t.Fatalf("failed to read any values")
	}
}

// Tests that duplicate point values are merged.  There is only one case
// where this could happen and that is when a compaction completed and we replace
// the old TSM file with a new one and we crash just before deleting the old file.
// No data is lost but the same point time/value would exist in two files until
// compaction corrects it.
func TestKeyIterator_TSM_Duplicate(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	v1 := tsm1.NewValue(time.Unix(1, 0), int64(1))
	v2 := tsm1.NewValue(time.Unix(1, 0), int64(1))

	writes := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": []tsm1.Value{v1},
	}

	r := MustTSMReader(dir, writes)

	iter, err := tsm1.NewTSMKeyIterator(r)
	if err != nil {
		t.Fatalf("unexpected error creating WALKeyIterator: %v", err)
	}

	var readValues bool
	for iter.Next() {
		key, values, err := iter.Read()
		if err != nil {
			t.Fatalf("unexpected error read: %v", err)
		}

		if got, exp := key, "cpu,host=A#!~#value"; got != exp {
			t.Fatalf("key mismatch: got %v, exp %v", got, exp)
		}

		if got, exp := len(values), 1; got != exp {
			t.Fatalf("values length mismatch: got %v, exp %v", got, exp)
		}

		readValues = true
		assertValueEqual(t, values[0], v2)
	}

	if !readValues {
		t.Fatalf("failed to read any values")
	}
}

// Tests that deleted keys are not seen during iteration with
// TSM files.
func TestKeyIterator_TSM_MultipleKeysDeleted(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	v1 := tsm1.NewValue(time.Unix(2, 0), int64(1))
	points1 := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": []tsm1.Value{v1},
	}

	r1 := MustTSMReader(dir, points1)
	r1.Delete("cpu,host=A#!~#value")

	v2 := tsm1.NewValue(time.Unix(1, 0), float64(1))
	v3 := tsm1.NewValue(time.Unix(1, 0), float64(1))

	points2 := map[string][]tsm1.Value{
		"cpu,host=A#!~#count": []tsm1.Value{v2},
		"cpu,host=B#!~#value": []tsm1.Value{v3},
	}

	r2 := MustTSMReader(dir, points2)
	r2.Delete("cpu,host=A#!~#count")

	iter, err := tsm1.NewTSMKeyIterator(r1, r2)
	if err != nil {
		t.Fatalf("unexpected error creating WALKeyIterator: %v", err)
	}

	var readValues bool
	var data = []struct {
		key   string
		value tsm1.Value
	}{
		{"cpu,host=B#!~#value", v3},
	}

	for iter.Next() {
		key, values, err := iter.Read()
		if err != nil {
			t.Fatalf("unexpected error read: %v", err)
		}

		if got, exp := key, data[0].key; got != exp {
			t.Fatalf("key mismatch: got %v, exp %v", got, exp)
		}

		if got, exp := len(values), 1; got != exp {
			t.Fatalf("values length mismatch: got %v, exp %v", got, exp)
		}
		readValues = true

		assertValueEqual(t, values[0], data[0].value)
		data = data[1:]
	}

	if !readValues {
		t.Fatalf("failed to read any values")
	}
}

func TestKeyIterator_Cache_Single(t *testing.T) {
	v0 := tsm1.NewValue(time.Unix(1, 0).UTC(), 1.0)

	writes := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": []tsm1.Value{v0},
	}

	c := tsm1.NewCache(0)

	for k, v := range writes {
		if err := c.Write(k, v); err != nil {
			t.Fatalf("failed to write key foo to cache: %s", err.Error())
		}
	}

	iter := tsm1.NewCacheKeyIterator(c)
	var readValues bool
	for iter.Next() {
		key, values, err := iter.Read()
		if err != nil {
			t.Fatalf("unexpected error read: %v", err)
		}

		if got, exp := key, "cpu,host=A#!~#value"; got != exp {
			t.Fatalf("key mismatch: got %v, exp %v", got, exp)
		}

		if got, exp := len(values), len(writes); got != exp {
			t.Fatalf("values length mismatch: got %v, exp %v", got, exp)
		}

		for _, v := range values {
			readValues = true
			assertValueEqual(t, v, v0)
		}
	}

	if !readValues {
		t.Fatalf("failed to read any values")
	}
}

func TestDefaultCompactionPlanner_OnlyTSM_MaxSize(t *testing.T) {
	cp := &tsm1.DefaultPlanner{
		FileStore: &fakeFileStore{
			PathsFn: func() []tsm1.FileStat {
				return []tsm1.FileStat{
					tsm1.FileStat{
						Path: "1.tsm1",
						Size: 1 * 1024 * 1024,
					},
					tsm1.FileStat{
						Path: "2.tsm1",
						Size: 1 * 1024 * 1024,
					},
					tsm1.FileStat{
						Path: "3.tsm",
						Size: 251 * 1024 * 1024,
					},
				}
			},
		},
	}

	tsm := cp.Plan()
	if exp, got := 2, len(tsm); got != exp {
		t.Fatalf("tsm file length mismatch: got %v, exp %v", got, exp)
	}
}

func TestDefaultCompactionPlanner_TSM_Rewrite(t *testing.T) {
	cp := &tsm1.DefaultPlanner{
		FileStore: &fakeFileStore{
			PathsFn: func() []tsm1.FileStat {
				return []tsm1.FileStat{
					tsm1.FileStat{
						Path: "0001.tsm1",
						Size: 1 * 1024 * 1024,
					},
					tsm1.FileStat{
						Path: "0002.tsm1",
						Size: 1 * 1024 * 1024,
					},
					tsm1.FileStat{
						Size: 251 * 1024 * 1024,
					},
				}
			},
		},
	}

	tsm := cp.Plan()
	if exp, got := 2, len(tsm); got != exp {
		t.Fatalf("tsm file length mismatch: got %v, exp %v", got, exp)
	}
}

func TestDefaultCompactionPlanner_Rewrite_Deletes(t *testing.T) {
	cp := &tsm1.DefaultPlanner{
		FileStore: &fakeFileStore{
			PathsFn: func() []tsm1.FileStat {
				return []tsm1.FileStat{
					tsm1.FileStat{
						Path:         "000007.tsm1",
						HasTombstone: true,
					},
					tsm1.FileStat{
						Size: 251 * 1024 * 1024,
					},
				}
			},
		},
	}

	tsm := cp.Plan()
	if exp, got := 1, len(tsm); got != exp {
		t.Fatalf("tsm file length mismatch: got %v, exp %v", got, exp)
	}
}

func assertValueEqual(t *testing.T, a, b tsm1.Value) {
	if got, exp := a.Time(), b.Time(); !got.Equal(exp) {
		t.Fatalf("time mismatch: got %v, exp %v", got, exp)
	}
	if got, exp := a.Value(), b.Value(); got != exp {
		t.Fatalf("value mismatch: got %v, exp %v", got, exp)
	}
}

func assertEqual(t *testing.T, a tsm1.Value, b models.Point, field string) {
	if got, exp := a.Time(), b.Time(); !got.Equal(exp) {
		t.Fatalf("time mismatch: got %v, exp %v", got, exp)
	}
	if got, exp := a.Value(), b.Fields()[field]; got != exp {
		t.Fatalf("value mismatch: got %v, exp %v", got, exp)
	}
}

func MustWALSegment(dir string, entries []tsm1.WALEntry) *tsm1.WALSegmentReader {
	f := MustTempFile(dir)
	w := tsm1.NewWALSegmentWriter(f)

	for _, e := range entries {
		if err := w.Write(e); err != nil {
			panic(fmt.Sprintf("write WAL entry: %v", err))
		}
	}

	if _, err := f.Seek(0, os.SEEK_SET); err != nil {
		panic(fmt.Sprintf("seek WAL: %v", err))
	}

	return tsm1.NewWALSegmentReader(f)
}

func MustTSMReader(dir string, values map[string][]tsm1.Value) *tsm1.TSMReader {
	f := MustTempFile(dir)
	w, err := tsm1.NewTSMWriter(f)
	if err != nil {
		panic(fmt.Sprintf("create TSM writer: %v", err))
	}

	for k, v := range values {
		if err := w.Write(k, v); err != nil {
			panic(fmt.Sprintf("write TSM value: %v", err))
		}
	}

	if err := w.WriteIndex(); err != nil {
		panic(fmt.Sprintf("write TSM index: %v", err))
	}

	if err := w.Close(); err != nil {
		panic(fmt.Sprintf("write TSM close: %v", err))
	}

	f, err = os.Open(f.Name())
	if err != nil {
		panic(fmt.Sprintf("open file: %v", err))
	}

	r, err := tsm1.NewTSMReaderWithOptions(
		tsm1.TSMReaderOptions{
			MMAPFile: f,
		})
	if err != nil {
		panic(fmt.Sprintf("new reader: %v", err))
	}
	return r
}

type fakeWAL struct {
	ClosedSegmentsFn func() ([]string, error)
}

func (w *fakeWAL) ClosedSegments() ([]string, error) {
	return w.ClosedSegmentsFn()
}

type fakeFileStore struct {
	PathsFn func() []tsm1.FileStat
}

func (w *fakeFileStore) Stats() []tsm1.FileStat {
	return w.PathsFn()
}

func (w *fakeFileStore) NextID() int {
	return 1
}
