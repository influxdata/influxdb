package tsm1_test

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"testing"

	"github.com/golang/snappy"
	"github.com/influxdata/influxdb/v2/pkg/slices"
	"github.com/influxdata/influxdb/v2/tsdb/engine/tsm1"
	"github.com/stretchr/testify/require"
)

func TestWALWriter_WriteMulti_Single(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	w := tsm1.NewWAL(dir, 0, 0)
	defer w.Close()
	require.NoError(t, w.Open())

	p1 := tsm1.NewValue(1, 1.1)
	p2 := tsm1.NewValue(1, int64(1))
	p3 := tsm1.NewValue(1, true)
	p4 := tsm1.NewValue(1, "string")
	p5 := tsm1.NewValue(1, ^uint64(0))

	values := map[string][]tsm1.Value{
		"cpu,host=A#!~#float":    {p1},
		"cpu,host=A#!~#int":      {p2},
		"cpu,host=A#!~#bool":     {p3},
		"cpu,host=A#!~#string":   {p4},
		"cpu,host=A#!~#unsigned": {p5},
	}

	_, err := w.WriteMulti(context.Background(), values)
	require.NoError(t, err)

	f, r := mustSegmentReader(t, w)
	defer r.Close()

	require.True(t, r.Next())

	we, err := r.Read()
	require.NoError(t, err)

	e, ok := we.(*tsm1.WriteWALEntry)
	require.True(t, ok)

	for k, v := range e.Values {
		for i, vv := range v {
			require.Equal(t, values[k][i].String(), vv.String())
		}
	}

	require.Equal(t, r.Count(), mustReadFileSize(f))
}

func TestWALWriter_WriteMulti_LargeBatch(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	w := tsm1.NewWAL(dir, 0, 0)
	defer w.Close()
	require.NoError(t, w.Open())

	var points []tsm1.Value
	for i := 0; i < 100000; i++ {
		points = append(points, tsm1.NewValue(int64(i), int64(1)))
	}

	values := map[string][]tsm1.Value{
		"cpu,host=A,server=01,foo=bar,tag=really-long#!~#float": points,
		"mem,host=A,server=01,foo=bar,tag=really-long#!~#float": points,
	}

	_, err := w.WriteMulti(context.Background(), values)
	require.NoError(t, err)

	f, r := mustSegmentReader(t, w)
	defer r.Close()

	require.True(t, r.Next())

	we, err := r.Read()
	require.NoError(t, err)

	e, ok := we.(*tsm1.WriteWALEntry)
	require.True(t, ok)

	for k, v := range e.Values {
		for i, vv := range v {
			require.Equal(t, values[k][i].String(), vv.String())
		}
	}

	require.Equal(t, r.Count(), mustReadFileSize(f))
}

func TestWALWriter_WriteMulti_Multiple(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	w := tsm1.NewWAL(dir, 0, 0)
	defer w.Close()
	require.NoError(t, w.Open())

	p1 := tsm1.NewValue(1, int64(1))
	p2 := tsm1.NewValue(1, int64(2))

	exp := []struct {
		key    string
		values []tsm1.Value
	}{
		{"cpu,host=A#!~#value", []tsm1.Value{p1}},
		{"cpu,host=B#!~#value", []tsm1.Value{p2}},
	}

	for _, v := range exp {
		_, err := w.WriteMulti(context.Background(), map[string][]tsm1.Value{v.key: v.values})
		require.NoError(t, err)
	}

	f, r := mustSegmentReader(t, w)
	defer r.Close()

	for _, ep := range exp {
		require.True(t, r.Next())

		we, err := r.Read()
		require.NoError(t, err)

		e, ok := we.(*tsm1.WriteWALEntry)
		require.True(t, ok)

		for k, v := range e.Values {
			require.Equal(t, k, ep.key)
			require.Equal(t, len(v), len(ep.values))

			for i, vv := range v {
				require.Equal(t, vv.String(), ep.values[i].String())
			}
		}
	}

	require.Equal(t, r.Count(), mustReadFileSize(f))
}

func TestWALWriter_WriteDelete_Single(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	w := tsm1.NewWAL(dir, 0, 0)
	defer w.Close()
	require.NoError(t, w.Open())

	keys := [][]byte{[]byte("cpu")}

	_, err := w.Delete(context.Background(), keys)
	require.NoError(t, err)

	_, r := mustSegmentReader(t, w)
	defer r.Close()

	require.True(t, r.Next())

	we, err := r.Read()
	require.NoError(t, err)

	e, ok := we.(*tsm1.DeleteWALEntry)
	require.True(t, ok)

	require.Equal(t, len(e.Keys), len(keys))
	require.Equal(t, string(e.Keys[0]), string(keys[0]))
}

func TestWALWriter_WriteMultiDelete_Multiple(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	w := tsm1.NewWAL(dir, 0, 0)
	defer w.Close()
	require.NoError(t, w.Open())

	p1 := tsm1.NewValue(1, true)
	values := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": {p1},
	}

	_, err := w.WriteMulti(context.Background(), values)
	require.NoError(t, err)

	deleteKeys := [][]byte{[]byte("cpu,host=A#!~value")}

	_, err = w.Delete(context.Background(), deleteKeys)
	require.NoError(t, err)

	_, r := mustSegmentReader(t, w)
	defer r.Close()

	require.True(t, r.Next())

	we, err := r.Read()
	require.NoError(t, err)

	e, ok := we.(*tsm1.WriteWALEntry)
	require.True(t, ok)

	for k, v := range e.Values {
		require.Equal(t, len(v), len(values[k]))

		for i, vv := range v {
			require.Equal(t, vv.String(), values[k][i].String())
		}
	}

	// Read the delete second
	require.True(t, r.Next())

	we, err = r.Read()
	require.NoError(t, err)

	de, ok := we.(*tsm1.DeleteWALEntry)
	require.True(t, ok)

	require.Equal(t, len(de.Keys), len(deleteKeys))
	require.Equal(t, string(de.Keys[0]), string(deleteKeys[0]))
}

func TestWALWriter_WriteMultiDeleteRange_Multiple(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	w := tsm1.NewWAL(dir, 0, 0)
	defer w.Close()
	require.NoError(t, w.Open())

	p1 := tsm1.NewValue(1, 1.0)
	p2 := tsm1.NewValue(2, 2.0)
	p3 := tsm1.NewValue(3, 3.0)

	values := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": {p1, p2, p3},
	}

	_, err := w.WriteMulti(context.Background(), values)
	require.NoError(t, err)

	// Write the delete entry
	deleteKeys := [][]byte{[]byte("cpu,host=A#!~value")}
	deleteMin, deleteMax := int64(2), int64(3)

	_, err = w.DeleteRange(context.Background(), deleteKeys, deleteMin, deleteMax)
	require.NoError(t, err)

	_, r := mustSegmentReader(t, w)
	defer r.Close()

	require.True(t, r.Next())

	we, err := r.Read()
	require.NoError(t, err)

	e, ok := we.(*tsm1.WriteWALEntry)
	require.True(t, ok)

	for k, v := range e.Values {
		require.Equal(t, len(v), len(values[k]))

		for i, vv := range v {
			require.Equal(t, vv.String(), values[k][i].String())
		}
	}

	// Read the delete second
	require.True(t, r.Next())

	we, err = r.Read()
	require.NoError(t, err)

	de, ok := we.(*tsm1.DeleteRangeWALEntry)
	require.True(t, ok)

	require.Equal(t, len(de.Keys), len(deleteKeys))
	require.Equal(t, string(de.Keys[0]), string(deleteKeys[0]))
	require.Equal(t, de.Min, deleteMin)
	require.Equal(t, de.Max, deleteMax)
}

func TestWAL_ClosedSegments(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	w := tsm1.NewWAL(dir, 0, 0)
	require.NoError(t, w.Open())

	files, err := w.ClosedSegments()
	require.NoError(t, err)

	require.Equal(t, len(files), 0)

	_, err = w.WriteMulti(context.Background(), map[string][]tsm1.Value{
		"cpu,host=A#!~#value": {
			tsm1.NewValue(1, 1.1),
		},
	})
	require.NoError(t, err)

	require.NoError(t, w.Close())

	// Re-open the WAL
	w = tsm1.NewWAL(dir, 0, 0)
	defer w.Close()
	require.NoError(t, w.Open())

	files, err = w.ClosedSegments()
	require.NoError(t, err)
	require.Equal(t, len(files), 0)
}

func TestWAL_Delete(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	w := tsm1.NewWAL(dir, 0, 0)
	require.NoError(t, w.Open())

	files, err := w.ClosedSegments()

	require.NoError(t, err)

	require.Equal(t, len(files), 0)

	_, err = w.Delete(context.Background(), [][]byte{[]byte("cpu")})
	require.NoError(t, err)

	require.NoError(t, w.Close())

	// Re-open the WAL
	w = tsm1.NewWAL(dir, 0, 0)
	defer w.Close()
	require.NoError(t, w.Open())

	files, err = w.ClosedSegments()
	require.NoError(t, err)
	require.Equal(t, len(files), 0)
}

func TestWALWriter_Corrupt(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	f := MustTempFile(dir)
	w := tsm1.NewWALSegmentWriter(f)
	corruption := []byte{1, 4, 0, 0, 0}

	p1 := tsm1.NewValue(1, 1.1)
	values := map[string][]tsm1.Value{
		"cpu,host=A#!~#float": {p1},
	}

	entry := &tsm1.WriteWALEntry{
		Values: values,
	}

	require.NoError(t, w.Write(mustMarshalEntry(entry)))
	require.NoError(t, w.Flush())

	// Write some random bytes to the file to simulate corruption.
	_, err := f.Write(corruption)
	require.NoError(t, err)

	// Create the WAL segment reader.
	_, err = f.Seek(0, io.SeekStart)
	require.NoError(t, err)

	r := tsm1.NewWALSegmentReader(f)

	// Try to decode two entries.
	require.True(t, r.Next())

	_, err = r.Read()
	require.NoError(t, err)

	require.True(t, r.Next())

	_, err = r.Read()
	require.Error(t, err)

	// Count should only return size of valid data.
	expCount := mustReadFileSize(f) - int64(len(corruption))
	require.Equal(t, expCount, r.Count())
}

// Reproduces a `panic: runtime error: makeslice: cap out of range` when run with
// GOARCH=386 go test -run TestWALSegmentReader_Corrupt -v ./tsdb/engine/tsm1/
func TestWALSegmentReader_Corrupt(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	f := MustTempFile(dir)
	w := tsm1.NewWALSegmentWriter(f)

	p4 := tsm1.NewValue(1, "string")

	values := map[string][]tsm1.Value{
		"cpu,host=A#!~#string": {p4, p4},
	}

	entry := &tsm1.WriteWALEntry{
		Values: values,
	}

	typ, b := mustMarshalEntry(entry)

	// This causes the nvals field to overflow on 32 bit systems which produces a
	// negative count and a panic when reading the segment.
	b[25] = 255

	require.NoError(t, w.Write(typ, b))
	require.NoError(t, w.Flush())

	// Create the WAL segment reader.
	_, err := f.Seek(0, io.SeekStart)
	require.NoError(t, err)

	r := tsm1.NewWALSegmentReader(f)
	defer r.Close()

	// Try to decode two entries.
	for r.Next() {
		r.Read()
	}
}

func TestWALRollSegment(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	w := tsm1.NewWAL(dir, 0, 0)
	require.NoError(t, w.Open())
	const segSize = 1024
	w.SegmentSize = segSize

	values := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": {tsm1.NewValue(1, 1.0)},
		"cpu,host=B#!~#value": {tsm1.NewValue(1, 1.0)},
		"cpu,host=C#!~#value": {tsm1.NewValue(1, 1.0)},
	}
	_, err := w.WriteMulti(context.Background(), values)
	require.NoError(t, err)

	files, err := os.ReadDir(w.Path())
	require.NoError(t, err)
	require.Equal(t, 1, len(files))

	file, err := files[0].Info()
	require.NoError(t, err)
	encodeSize := file.Size()

	for i := 0; i < 100; i++ {
		_, err := w.WriteMulti(context.Background(), values)
		require.NoError(t, err)
	}
	files, err = os.ReadDir(w.Path())
	require.NoError(t, err)
	for _, f := range files {
		file, err := f.Info()
		require.NoError(t, err)
		require.True(t, file.Size() <= int64(segSize)+encodeSize)
	}
	require.NoError(t, w.Close())
}

func TestWAL_DiskSize(t *testing.T) {
	test := func(w *tsm1.WAL, oldZero, curZero bool) {
		// get disk size by reading file
		files, err := os.ReadDir(w.Path())
		require.NoError(t, err)

		sort.Slice(files, func(i, j int) bool {
			return files[i].Name() < files[j].Name()
		})

		var old, cur int64
		if len(files) > 0 {
			file, err := files[len(files)-1].Info()
			require.NoError(t, err)
			cur = file.Size()
			for i := 0; i < len(files)-1; i++ {
				file, err := files[i].Info()
				require.NoError(t, err)
				old += file.Size()
			}
		}

		// test zero size condition
		require.False(t, oldZero && old > 0)
		require.False(t, !oldZero && old == 0)
		require.False(t, curZero && cur > 0)
		require.False(t, !curZero && cur == 0)

		// test method DiskSizeBytes
		require.Equal(t, old+cur, w.DiskSizeBytes(), "total disk size")

		// test Statistics
		ss := w.Statistics(nil)
		require.Equal(t, 1, len(ss))

		m := ss[0].Values
		require.NotNil(t, m)

		require.Equal(t, m["oldSegmentsDiskBytes"].(int64), old, "old disk size")
		require.Equal(t, m["currentSegmentDiskBytes"].(int64), cur, "current dist size")
	}

	dir := MustTempDir()
	defer os.RemoveAll(dir)

	w := tsm1.NewWAL(dir, 0, 0)

	const segSize = 1024
	w.SegmentSize = segSize

	// open
	require.NoError(t, w.Open())

	test(w, true, true)

	// write some values, the total size of these values does not exceed segSize(1024),
	// so rollSegment will not be triggered
	values := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": {tsm1.NewValue(1, 1.0)},
		"cpu,host=B#!~#value": {tsm1.NewValue(1, 1.0)},
		"cpu,host=C#!~#value": {tsm1.NewValue(1, 1.0)},
	}

	_, err := w.WriteMulti(context.Background(), values)
	require.NoError(t, err)

	test(w, true, false)

	// write some values, the total size of these values exceeds segSize(1024),
	// so rollSegment will be triggered
	for i := 0; i < 100; i++ {
		_, err := w.WriteMulti(context.Background(), values)
		require.NoError(t, err)
	}

	test(w, false, false)

	// reopen
	require.NoError(t, w.Close())
	require.NoError(t, w.Open())

	test(w, false, false)

	// remove
	closedSegments, err := w.ClosedSegments()
	require.NoError(t, err)
	require.NoError(t, w.Remove(closedSegments))

	test(w, true, false)
}

func TestWriteWALSegment_UnmarshalBinary_WriteWALCorrupt(t *testing.T) {
	p1 := tsm1.NewValue(1, 1.1)
	p2 := tsm1.NewValue(1, int64(1))
	p3 := tsm1.NewValue(1, true)
	p4 := tsm1.NewValue(1, "string")
	p5 := tsm1.NewValue(1, uint64(1))

	values := map[string][]tsm1.Value{
		"cpu,host=A#!~#float":    {p1, p1},
		"cpu,host=A#!~#int":      {p2, p2},
		"cpu,host=A#!~#bool":     {p3, p3},
		"cpu,host=A#!~#string":   {p4, p4},
		"cpu,host=A#!~#unsigned": {p5, p5},
	}

	w := &tsm1.WriteWALEntry{
		Values: values,
	}

	b, err := w.MarshalBinary()
	require.NoError(t, err)

	// Test every possible truncation of a write WAL entry
	for i := 0; i < len(b); i++ {
		// re-allocated to ensure capacity would be exceed if slicing
		truncated := make([]byte, i)
		copy(truncated, b[:i])
		err := w.UnmarshalBinary(truncated)
		require.True(t, err == nil || err == tsm1.ErrWALCorrupt)
	}
}

func TestDeleteWALEntry_UnmarshalBinary(t *testing.T) {
	examples := []struct {
		In  []string
		Out [][]byte
	}{
		{
			In:  []string{""},
			Out: nil,
		},
		{
			In:  []string{"foo"},
			Out: [][]byte{[]byte("foo")},
		},
		{
			In:  []string{"foo", "bar"},
			Out: [][]byte{[]byte("foo"), []byte("bar")},
		},
		{
			In:  []string{"foo", "bar", "z", "abc"},
			Out: [][]byte{[]byte("foo"), []byte("bar"), []byte("z"), []byte("abc")},
		},
		{
			In:  []string{"foo", "bar", "z", "a"},
			Out: [][]byte{[]byte("foo"), []byte("bar"), []byte("z"), []byte("a")},
		},
	}

	for _, example := range examples {
		w := &tsm1.DeleteWALEntry{Keys: slices.StringsToBytes(example.In...)}
		b, err := w.MarshalBinary()
		require.NoError(t, err)

		out := &tsm1.DeleteWALEntry{}
		require.NoError(t, out.UnmarshalBinary(b))

		require.True(t, reflect.DeepEqual(example.Out, out.Keys))
	}
}

func TestWriteWALSegment_UnmarshalBinary_DeleteWALCorrupt(t *testing.T) {
	w := &tsm1.DeleteWALEntry{
		Keys: [][]byte{[]byte("foo"), []byte("bar")},
	}

	b, err := w.MarshalBinary()
	require.NoError(t, err)

	// Test every possible truncation of a write WAL entry
	for i := 0; i < len(b); i++ {
		// re-allocated to ensure capacity would be exceed if slicing
		truncated := make([]byte, i)
		copy(truncated, b[:i])
		err := w.UnmarshalBinary(truncated)
		require.True(t, err == nil || err == tsm1.ErrWALCorrupt)
	}
}

func TestWriteWALSegment_UnmarshalBinary_DeleteRangeWALCorrupt(t *testing.T) {
	w := &tsm1.DeleteRangeWALEntry{
		Keys: [][]byte{[]byte("foo"), []byte("bar")},
		Min:  1,
		Max:  2,
	}

	b, err := w.MarshalBinary()
	require.NoError(t, err)

	// Test every possible truncation of a write WAL entry
	for i := 0; i < len(b); i++ {
		// re-allocated to ensure capacity would be exceed if slicing
		truncated := make([]byte, i)
		copy(truncated, b[:i])
		err := w.UnmarshalBinary(truncated)
		require.True(t, err == nil || err == tsm1.ErrWALCorrupt)
	}
}

func BenchmarkWAL_WriteMulti_Concurrency(b *testing.B) {
	benchmarks := []struct {
		concurrency int
	}{
		{1},
		{12},
		{24},
		{50},
		{100},
		{200},
		{300},
		{400},
		{500},
	}

	for _, bm := range benchmarks {
		b.Run(fmt.Sprintf("concurrency-%d", bm.concurrency), func(b *testing.B) {
			points := map[string][]tsm1.Value{}
			for i := 0; i < 5000; i++ {
				k := "cpu,host=A#!~#value"
				points[k] = append(points[k], tsm1.NewValue(int64(i), 1.1))
			}

			dir := MustTempDir()
			defer os.RemoveAll(dir)

			w := tsm1.NewWAL(dir, 0, 0)
			defer w.Close()
			require.NoError(b, w.Open())

			start := make(chan struct{})
			stop := make(chan struct{})

			succeed := make(chan struct{}, 1000)
			defer close(succeed)

			wg := &sync.WaitGroup{}
			for i := 0; i < bm.concurrency; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()

					<-start

					for {
						select {
						case <-stop:
							return
						default:
							_, err := w.WriteMulti(context.Background(), points)
							require.NoError(b, err)

							succeed <- struct{}{}
						}
					}
				}()
			}

			b.ResetTimer()

			close(start)

			for i := 0; i < b.N; i++ {
				<-succeed
			}

			b.StopTimer()

			close(stop)
			wg.Wait()
		})
	}
}

func BenchmarkWALSegmentWriter(b *testing.B) {
	points := map[string][]tsm1.Value{}
	for i := 0; i < 5000; i++ {
		k := "cpu,host=A#!~#value"
		points[k] = append(points[k], tsm1.NewValue(int64(i), 1.1))
	}

	dir := MustTempDir()
	defer os.RemoveAll(dir)

	f := MustTempFile(dir)
	w := tsm1.NewWALSegmentWriter(f)

	write := &tsm1.WriteWALEntry{
		Values: points,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		require.NoError(b, w.Write(mustMarshalEntry(write)))
	}
}

func BenchmarkWALSegmentReader(b *testing.B) {
	points := map[string][]tsm1.Value{}
	for i := 0; i < 5000; i++ {
		k := "cpu,host=A#!~#value"
		points[k] = append(points[k], tsm1.NewValue(int64(i), 1.1))
	}

	dir := MustTempDir()
	defer os.RemoveAll(dir)

	f := MustTempFile(dir)
	w := tsm1.NewWALSegmentWriter(f)

	write := &tsm1.WriteWALEntry{
		Values: points,
	}

	for i := 0; i < 100; i++ {
		require.NoError(b, w.Write(mustMarshalEntry(write)))
	}

	r := tsm1.NewWALSegmentReader(f)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		f.Seek(0, io.SeekStart)
		b.StartTimer()

		for r.Next() {
			_, err := r.Read()
			require.NoError(b, err)
		}
	}
}

func mustSegmentReader(t *testing.T, w *tsm1.WAL) (*os.File, *tsm1.WALSegmentReader) {
	files, err := filepath.Glob(filepath.Join(w.Path(),
		fmt.Sprintf("%s*.%s", tsm1.WALFilePrefix, tsm1.WALFileExtension)))
	require.NoError(t, err)
	require.Equal(t, 1, len(files))

	sort.Strings(files)

	f, err := os.OpenFile(files[0], os.O_CREATE|os.O_RDWR, 0666)
	require.NoError(t, err)
	return f, tsm1.NewWALSegmentReader(f)
}

// mustReadFileSize returns the size of the file, or panics.
func mustReadFileSize(f *os.File) int64 {
	stat, err := os.Stat(f.Name())
	if err != nil {
		panic(fmt.Sprintf("failed to get size of file at %s: %s", f.Name(), err.Error()))
	}
	return stat.Size()
}

func mustMarshalEntry(entry tsm1.WALEntry) (tsm1.WalEntryType, []byte) {
	bytes := make([]byte, 1024<<2)

	b, err := entry.Encode(bytes)
	if err != nil {
		panic(fmt.Sprintf("error encoding: %v", err))
	}

	return entry.Type(), snappy.Encode(b, b)
}
