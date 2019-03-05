package wal

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"reflect"
	"testing"

	"github.com/golang/snappy"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/tsdb/value"
)

func TestWALWriter_WriteMulti_Single(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	f := MustTempFile(dir)
	w := NewWALSegmentWriter(f)

	p1 := value.NewValue(1, 1.1)
	p2 := value.NewValue(1, int64(1))
	p3 := value.NewValue(1, true)
	p4 := value.NewValue(1, "string")
	p5 := value.NewValue(1, ^uint64(0))

	values := map[string][]value.Value{
		"cpu,host=A#!~#float":    []value.Value{p1},
		"cpu,host=A#!~#int":      []value.Value{p2},
		"cpu,host=A#!~#bool":     []value.Value{p3},
		"cpu,host=A#!~#string":   []value.Value{p4},
		"cpu,host=A#!~#unsigned": []value.Value{p5},
	}

	entry := &WriteWALEntry{
		Values: values,
	}

	if err := w.Write(mustMarshalEntry(entry)); err != nil {
		fatal(t, "write points", err)
	}

	if err := w.Flush(); err != nil {
		fatal(t, "flush", err)
	}

	if _, err := f.Seek(0, io.SeekStart); err != nil {
		fatal(t, "seek", err)
	}

	r := NewWALSegmentReader(f)

	if !r.Next() {
		t.Fatalf("expected next, got false")
	}

	we, err := r.Read()
	if err != nil {
		fatal(t, "read entry", err)
	}

	e, ok := we.(*WriteWALEntry)
	if !ok {
		t.Fatalf("expected WriteWALEntry: got %#v", e)
	}

	for k, v := range e.Values {
		for i, vv := range v {
			if got, exp := vv.String(), values[k][i].String(); got != exp {
				t.Fatalf("points mismatch: got %v, exp %v", got, exp)
			}
		}
	}

	if n := r.Count(); n != MustReadFileSize(f) {
		t.Fatalf("wrong count of bytes read, got %d, exp %d", n, MustReadFileSize(f))
	}
}

func TestWALWriter_WriteMulti_LargeBatch(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	f := MustTempFile(dir)
	w := NewWALSegmentWriter(f)

	var points []value.Value
	for i := 0; i < 100000; i++ {
		points = append(points, value.NewValue(int64(i), int64(1)))
	}

	values := map[string][]value.Value{
		"cpu,host=A,server=01,foo=bar,tag=really-long#!~#float": points,
		"mem,host=A,server=01,foo=bar,tag=really-long#!~#float": points,
	}

	entry := &WriteWALEntry{
		Values: values,
	}

	if err := w.Write(mustMarshalEntry(entry)); err != nil {
		fatal(t, "write points", err)
	}

	if err := w.Flush(); err != nil {
		fatal(t, "flush", err)
	}

	if _, err := f.Seek(0, io.SeekStart); err != nil {
		fatal(t, "seek", err)
	}

	r := NewWALSegmentReader(f)

	if !r.Next() {
		t.Fatalf("expected next, got false")
	}

	we, err := r.Read()
	if err != nil {
		fatal(t, "read entry", err)
	}

	e, ok := we.(*WriteWALEntry)
	if !ok {
		t.Fatalf("expected WriteWALEntry: got %#v", e)
	}

	for k, v := range e.Values {
		for i, vv := range v {
			if got, exp := vv.String(), values[k][i].String(); got != exp {
				t.Fatalf("points mismatch: got %v, exp %v", got, exp)
			}
		}
	}

	if n := r.Count(); n != MustReadFileSize(f) {
		t.Fatalf("wrong count of bytes read, got %d, exp %d", n, MustReadFileSize(f))
	}
}

func TestWALWriter_WriteMulti_Multiple(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	f := MustTempFile(dir)
	w := NewWALSegmentWriter(f)

	p1 := value.NewValue(1, int64(1))
	p2 := value.NewValue(1, int64(2))

	exp := []struct {
		key    string
		values []value.Value
	}{
		{"cpu,host=A#!~#value", []value.Value{p1}},
		{"cpu,host=B#!~#value", []value.Value{p2}},
	}

	for _, v := range exp {
		entry := &WriteWALEntry{
			Values: map[string][]value.Value{v.key: v.values},
		}

		if err := w.Write(mustMarshalEntry(entry)); err != nil {
			fatal(t, "write points", err)
		}
		if err := w.Flush(); err != nil {
			fatal(t, "flush", err)
		}
	}

	// Seek back to the beinning of the file for reading
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		fatal(t, "seek", err)
	}

	r := NewWALSegmentReader(f)

	for _, ep := range exp {
		if !r.Next() {
			t.Fatalf("expected next, got false")
		}

		we, err := r.Read()
		if err != nil {
			fatal(t, "read entry", err)
		}

		e, ok := we.(*WriteWALEntry)
		if !ok {
			t.Fatalf("expected WriteWALEntry: got %#v", e)
		}

		for k, v := range e.Values {
			if got, exp := k, ep.key; got != exp {
				t.Fatalf("key mismatch. got %v, exp %v", got, exp)
			}

			if got, exp := len(v), len(ep.values); got != exp {
				t.Fatalf("values length mismatch: got %v, exp %v", got, exp)
			}

			for i, vv := range v {
				if got, exp := vv.String(), ep.values[i].String(); got != exp {
					t.Fatalf("points mismatch: got %v, exp %v", got, exp)
				}
			}
		}
	}

	if n := r.Count(); n != MustReadFileSize(f) {
		t.Fatalf("wrong count of bytes read, got %d, exp %d", n, MustReadFileSize(f))
	}
}

func TestWALWriter_DeleteBucketRange(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	f := MustTempFile(dir)
	w := NewWALSegmentWriter(f)

	entry := &DeleteBucketRangeWALEntry{
		OrgID:    influxdb.ID(1),
		BucketID: influxdb.ID(2),
		Min:      3,
		Max:      4,
	}

	if err := w.Write(mustMarshalEntry(entry)); err != nil {
		fatal(t, "write points", err)
	}

	if err := w.Flush(); err != nil {
		fatal(t, "flush", err)
	}

	if _, err := f.Seek(0, io.SeekStart); err != nil {
		fatal(t, "seek", err)
	}

	r := NewWALSegmentReader(f)

	if !r.Next() {
		t.Fatalf("expected next, got false")
	}

	we, err := r.Read()
	if err != nil {
		fatal(t, "read entry", err)
	}

	e, ok := we.(*DeleteBucketRangeWALEntry)
	if !ok {
		t.Fatalf("expected WriteWALEntry: got %#v", e)
	}

	if !reflect.DeepEqual(entry, e) {
		t.Fatalf("expected %+v but got %+v", entry, e)
	}
}

func TestWAL_ClosedSegments(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	w := NewWAL(dir)
	if err := w.Open(context.Background()); err != nil {
		t.Fatalf("error opening WAL: %v", err)
	}

	files, err := w.ClosedSegments()
	if err != nil {
		t.Fatalf("error getting closed segments: %v", err)
	}

	if got, exp := len(files), 0; got != exp {
		t.Fatalf("close segment length mismatch: got %v, exp %v", got, exp)
	}

	if _, err := w.WriteMulti(context.Background(), map[string][]value.Value{
		"cpu,host=A#!~#value": []value.Value{
			value.NewValue(1, 1.1),
		},
	}); err != nil {
		t.Fatalf("error writing points: %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("error closing wal: %v", err)
	}

	// Re-open the WAL
	w = NewWAL(dir)
	defer w.Close()
	if err := w.Open(context.Background()); err != nil {
		t.Fatalf("error opening WAL: %v", err)
	}

	files, err = w.ClosedSegments()
	if err != nil {
		t.Fatalf("error getting closed segments: %v", err)
	}
	if got, exp := len(files), 0; got != exp {
		t.Fatalf("close segment length mismatch: got %v, exp %v", got, exp)
	}
}

func TestWALWriter_Corrupt(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	f := MustTempFile(dir)
	w := NewWALSegmentWriter(f)
	corruption := []byte{1, 4, 0, 0, 0}

	p1 := value.NewValue(1, 1.1)
	values := map[string][]value.Value{
		"cpu,host=A#!~#float": []value.Value{p1},
	}

	entry := &WriteWALEntry{
		Values: values,
	}
	if err := w.Write(mustMarshalEntry(entry)); err != nil {
		fatal(t, "write points", err)
	}

	if err := w.Flush(); err != nil {
		fatal(t, "flush", err)
	}

	// Write some random bytes to the file to simulate corruption.
	if _, err := f.Write(corruption); err != nil {
		fatal(t, "corrupt WAL segment", err)
	}

	// Create the WAL segment reader.
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		fatal(t, "seek", err)
	}
	r := NewWALSegmentReader(f)

	// Try to decode two entries.

	if !r.Next() {
		t.Fatalf("expected next, got false")
	}
	if _, err := r.Read(); err != nil {
		fatal(t, "read entry", err)
	}

	if !r.Next() {
		t.Fatalf("expected next, got false")
	}
	if _, err := r.Read(); err == nil {
		fatal(t, "read entry did not return err", nil)
	}

	// Count should only return size of valid data.
	expCount := MustReadFileSize(f) - int64(len(corruption))
	if n := r.Count(); n != expCount {
		t.Fatalf("wrong count of bytes read, got %d, exp %d", n, expCount)
	}
}

// Reproduces a `panic: runtime error: makeslice: cap out of range` when run with
// GOARCH=386 go test -run TestWALSegmentReader_Corrupt -v ./tsdb/engine/tsm1/
func TestWALSegmentReader_Corrupt(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	f := MustTempFile(dir)
	w := NewWALSegmentWriter(f)

	p4 := value.NewValue(1, "string")

	values := map[string][]value.Value{
		"cpu,host=A#!~#string": []value.Value{p4, p4},
	}

	entry := &WriteWALEntry{
		Values: values,
	}

	typ, b := mustMarshalEntry(entry)

	// This causes the nvals field to overflow on 32 bit systems which produces a
	// negative count and a panic when reading the segment.
	b[25] = 255

	if err := w.Write(typ, b); err != nil {
		fatal(t, "write points", err)
	}

	if err := w.Flush(); err != nil {
		fatal(t, "flush", err)
	}

	// Create the WAL segment reader.
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		fatal(t, "seek", err)
	}

	r := NewWALSegmentReader(f)
	defer r.Close()

	// Try to decode two entries.
	for r.Next() {
		r.Read()
	}
}

func TestWriteWALSegment_UnmarshalBinary_WriteWALCorrupt(t *testing.T) {
	p1 := value.NewValue(1, 1.1)
	p2 := value.NewValue(1, int64(1))
	p3 := value.NewValue(1, true)
	p4 := value.NewValue(1, "string")
	p5 := value.NewValue(1, uint64(1))

	values := map[string][]value.Value{
		"cpu,host=A#!~#float":    []value.Value{p1, p1},
		"cpu,host=A#!~#int":      []value.Value{p2, p2},
		"cpu,host=A#!~#bool":     []value.Value{p3, p3},
		"cpu,host=A#!~#string":   []value.Value{p4, p4},
		"cpu,host=A#!~#unsigned": []value.Value{p5, p5},
	}

	w := &WriteWALEntry{
		Values: values,
	}

	b, err := w.MarshalBinary()
	if err != nil {
		t.Fatalf("unexpected error, got %v", err)
	}

	// Test every possible truncation of a write WAL entry
	for i := 0; i < len(b); i++ {
		// re-allocated to ensure capacity would be exceed if slicing
		truncated := make([]byte, i)
		copy(truncated, b[:i])
		err := w.UnmarshalBinary(truncated)
		if err != nil && err != ErrWALCorrupt {
			t.Fatalf("unexpected error: %v", err)
		}
	}
}

func TestDeleteBucketRangeWALEntry_UnmarshalBinary(t *testing.T) {
	for i := 0; i < 1000; i++ {
		in := &DeleteBucketRangeWALEntry{
			OrgID:    influxdb.ID(rand.Int63()) + 1,
			BucketID: influxdb.ID(rand.Int63()) + 1,
			Min:      rand.Int63(),
			Max:      rand.Int63(),
		}

		b, err := in.MarshalBinary()
		if err != nil {
			t.Fatalf("unexpected error, got %v", err)
		}

		out := &DeleteBucketRangeWALEntry{}
		if err := out.UnmarshalBinary(b); err != nil {
			t.Fatalf("%v", err)
		}

		if !reflect.DeepEqual(in, out) {
			t.Errorf("got %+v, expected %+v", out, in)
		}
	}
}

func TestWriteWALSegment_UnmarshalBinary_DeleteBucketRangeWALCorrupt(t *testing.T) {
	w := &DeleteBucketRangeWALEntry{
		OrgID:    influxdb.ID(1),
		BucketID: influxdb.ID(2),
		Min:      3,
		Max:      4,
	}

	b, err := w.MarshalBinary()
	if err != nil {
		t.Fatalf("unexpected error, got %v", err)
	}

	// Test every possible truncation of a write WAL entry
	for i := 0; i < len(b); i++ {
		// re-allocated to ensure capacity would be exceed if slicing
		truncated := make([]byte, i)
		copy(truncated, b[:i])
		err := w.UnmarshalBinary(truncated)
		if err != nil && err != ErrWALCorrupt {
			t.Fatalf("unexpected error: %v", err)
		}
	}
}

func BenchmarkWALSegmentWriter(b *testing.B) {
	points := map[string][]value.Value{}
	for i := 0; i < 5000; i++ {
		k := "cpu,host=A#!~#value"
		points[k] = append(points[k], value.NewValue(int64(i), 1.1))
	}

	dir := MustTempDir()
	defer os.RemoveAll(dir)

	f := MustTempFile(dir)
	w := NewWALSegmentWriter(f)

	write := &WriteWALEntry{
		Values: points,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := w.Write(mustMarshalEntry(write)); err != nil {
			b.Fatalf("unexpected error writing entry: %v", err)
		}
	}
}

func BenchmarkWALSegmentReader(b *testing.B) {
	points := map[string][]value.Value{}
	for i := 0; i < 5000; i++ {
		k := "cpu,host=A#!~#value"
		points[k] = append(points[k], value.NewValue(int64(i), 1.1))
	}

	dir := MustTempDir()
	defer os.RemoveAll(dir)

	f := MustTempFile(dir)
	w := NewWALSegmentWriter(f)

	write := &WriteWALEntry{
		Values: points,
	}

	for i := 0; i < 100; i++ {
		if err := w.Write(mustMarshalEntry(write)); err != nil {
			b.Fatalf("unexpected error writing entry: %v", err)
		}
	}

	r := NewWALSegmentReader(f)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		f.Seek(0, io.SeekStart)
		b.StartTimer()

		for r.Next() {
			_, err := r.Read()
			if err != nil {
				b.Fatalf("unexpected error reading entry: %v", err)
			}
		}
	}
}

// MustReadFileSize returns the size of the file, or panics.
func MustReadFileSize(f *os.File) int64 {
	stat, err := os.Stat(f.Name())
	if err != nil {
		panic(fmt.Sprintf("failed to get size of file at %s: %s", f.Name(), err.Error()))
	}
	return stat.Size()
}

func mustMarshalEntry(entry WALEntry) (WalEntryType, []byte) {
	bytes := make([]byte, 1024<<2)

	b, err := entry.Encode(bytes)
	if err != nil {
		panic(fmt.Sprintf("error encoding: %v", err))
	}

	return entry.Type(), snappy.Encode(b, b)
}
