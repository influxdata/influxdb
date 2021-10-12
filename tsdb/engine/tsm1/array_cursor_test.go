package tsm1

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/v2/pkg/fs"
	"github.com/influxdata/influxdb/v2/tsdb/cursors"
	"github.com/stretchr/testify/assert"
)

type keyValues struct {
	key    string
	values []Value
}

func MustTempDir() string {
	dir, err := os.MkdirTemp("", "tsm1-test")
	if err != nil {
		panic(fmt.Sprintf("failed to create temp dir: %v", err))
	}
	return dir
}

func MustTempFile(dir string) *os.File {
	f, err := os.CreateTemp(dir, "tsm1test")
	if err != nil {
		panic(fmt.Sprintf("failed to create temp file: %v", err))
	}
	return f
}

func newFiles(dir string, values ...keyValues) ([]string, error) {
	var files []string

	id := 1
	for _, v := range values {
		f := MustTempFile(dir)
		w, err := NewTSMWriter(f)
		if err != nil {
			return nil, err
		}

		if err := w.Write([]byte(v.key), v.values); err != nil {
			return nil, err
		}

		if err := w.WriteIndex(); err != nil {
			return nil, err
		}

		if err := w.Close(); err != nil {
			return nil, err
		}

		newName := filepath.Join(filepath.Dir(f.Name()), DefaultFormatFileName(id, 1)+".tsm")
		if err := fs.RenameFile(f.Name(), newName); err != nil {
			return nil, err
		}
		id++

		files = append(files, newName)
	}
	return files, nil
}

func TestDescendingCursor_SinglePointStartTime(t *testing.T) {
	t.Run("cache", func(t *testing.T) {
		dir := MustTempDir()
		defer os.RemoveAll(dir)
		fs := NewFileStore(dir)

		const START, END = 10, 1
		kc := fs.KeyCursor(context.Background(), []byte("m,_field=v#!~#v"), START, false)
		defer kc.Close()
		cur := newIntegerArrayDescendingCursor()
		// Include a cached value with timestamp equal to END
		cur.reset(START, END, Values{NewIntegerValue(1, 1)}, kc)

		var got []int64
		ar := cur.Next()
		for ar.Len() > 0 {
			got = append(got, ar.Timestamps...)
			ar = cur.Next()
		}

		if exp := []int64{1}; !cmp.Equal(got, exp) {
			t.Errorf("unexpected values; -got/+exp\n%s", cmp.Diff(got, exp))
		}
	})
	t.Run("tsm", func(t *testing.T) {
		dir := MustTempDir()
		defer os.RemoveAll(dir)
		fs := NewFileStore(dir)

		const START, END = 10, 1

		data := []keyValues{
			// Write a single data point with timestamp equal to END
			{"m,_field=v#!~#v", []Value{NewIntegerValue(1, 1)}},
		}

		files, err := newFiles(dir, data...)
		if err != nil {
			t.Fatalf("unexpected error creating files: %v", err)
		}

		_ = fs.Replace(nil, files)

		kc := fs.KeyCursor(context.Background(), []byte("m,_field=v#!~#v"), START, false)
		defer kc.Close()
		cur := newIntegerArrayDescendingCursor()
		cur.reset(START, END, nil, kc)

		var got []int64
		ar := cur.Next()
		for ar.Len() > 0 {
			got = append(got, ar.Timestamps...)
			ar = cur.Next()
		}

		if exp := []int64{1}; !cmp.Equal(got, exp) {
			t.Errorf("unexpected values; -got/+exp\n%s", cmp.Diff(got, exp))
		}
	})
}

func TestFileStore_DuplicatePoints(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := NewFileStore(dir)

	makeVals := func(ts ...int64) []Value {
		vals := make([]Value, len(ts))
		for i, t := range ts {
			vals[i] = NewFloatValue(t, 1.01)
		}
		return vals
	}

	// Setup 3 files
	data := []keyValues{
		{"m,_field=v#!~#v", makeVals(21)},
		{"m,_field=v#!~#v", makeVals(44)},
		{"m,_field=v#!~#v", makeVals(40, 46)},
		{"m,_field=v#!~#v", makeVals(46, 51)},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	_ = fs.Replace(nil, files)

	t.Run("ascending", func(t *testing.T) {
		const START, END = 0, 100
		kc := fs.KeyCursor(context.Background(), []byte("m,_field=v#!~#v"), START, true)
		defer kc.Close()
		cur := newFloatArrayAscendingCursor()
		cur.reset(START, END, nil, kc)

		var got []int64
		ar := cur.Next()
		for ar.Len() > 0 {
			got = append(got, ar.Timestamps...)
			ar = cur.Next()
		}

		if exp := []int64{21, 40, 44, 46, 51}; !cmp.Equal(got, exp) {
			t.Errorf("unexpected values; -got/+exp\n%s", cmp.Diff(got, exp))
		}
	})

	t.Run("descending", func(t *testing.T) {
		const START, END = 100, 0
		kc := fs.KeyCursor(context.Background(), []byte("m,_field=v#!~#v"), START, false)
		defer kc.Close()
		cur := newFloatArrayDescendingCursor()
		cur.reset(START, END, nil, kc)

		var got []int64
		ar := cur.Next()
		for ar.Len() > 0 {
			got = append(got, ar.Timestamps...)
			ar = cur.Next()
		}

		if exp := []int64{51, 46, 44, 40, 21}; !cmp.Equal(got, exp) {
			t.Errorf("unexpected values; -got/+exp\n%s", cmp.Diff(got, exp))
		}
	})
}

// Int64Slice attaches the methods of Interface to []int64, sorting in increasing order.
type Int64Slice []int64

func (p Int64Slice) Len() int           { return len(p) }
func (p Int64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p Int64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

// Verifies the array cursors correctly handle merged blocks from KeyCursor which may exceed the
// array cursor's local values buffer, which is initialized to MaxPointsPerBlock elements (1000)
//
// This test creates two TSM files which have a single block each. The second file
// has interleaving timestamps with the first file.
//
// The first file has a block of 800 timestamps starting at 1000 an increasing by 10ns
// The second file has a block of 400 timestamps starting at 1005, also increasing by 10ns
//
// When calling `nextTSM`, a single block of 1200 timestamps will be returned and the
// array cursor must chuck the values in the Next call.
func TestFileStore_MergeBlocksLargerThat1000_SecondEntirelyContained(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := NewFileStore(dir)

	// makeVals creates count points starting at ts and incrementing by step
	makeVals := func(ts, count, step int64) []Value {
		vals := make([]Value, count)
		for i := range vals {
			vals[i] = NewFloatValue(ts, 1.01)
			ts += step
		}
		return vals
	}

	makeTs := func(ts, count, step int64) []int64 {
		vals := make([]int64, count)
		for i := range vals {
			vals[i] = ts
			ts += step
		}
		return vals
	}

	// Setup 2 files with the second containing a single block that is completely within the first
	data := []keyValues{
		{"m,_field=v#!~#v", makeVals(1000, 800, 10)},
		{"m,_field=v#!~#v", makeVals(1005, 400, 10)},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	_ = fs.Replace(nil, files)

	t.Run("ascending", func(t *testing.T) {
		const START, END = 1000, 10000
		kc := fs.KeyCursor(context.Background(), []byte("m,_field=v#!~#v"), START, true)
		defer kc.Close()
		cur := newFloatArrayAscendingCursor()
		cur.reset(START, END, nil, kc)

		exp := makeTs(1000, 800, 10)
		exp = append(exp, makeTs(1005, 400, 10)...)
		sort.Sort(Int64Slice(exp))

		// check first block
		ar := cur.Next()
		assert.Len(t, ar.Timestamps, 1000)
		assert.Equal(t, exp[:1000], ar.Timestamps)

		// check second block
		exp = exp[1000:]
		ar = cur.Next()
		assert.Len(t, ar.Timestamps, 200)
		assert.Equal(t, exp, ar.Timestamps)
	})

	t.Run("descending", func(t *testing.T) {
		const START, END = 10000, 0
		kc := fs.KeyCursor(context.Background(), []byte("m,_field=v#!~#v"), START, false)
		defer kc.Close()
		cur := newFloatArrayDescendingCursor()
		cur.reset(START, END, nil, kc)

		exp := makeTs(1000, 800, 10)
		exp = append(exp, makeTs(1005, 400, 10)...)
		sort.Sort(sort.Reverse(Int64Slice(exp)))

		// check first block
		ar := cur.Next()
		assert.Len(t, ar.Timestamps, 1000)
		assert.Equal(t, exp[:1000], ar.Timestamps)

		// check second block
		exp = exp[1000:]
		ar = cur.Next()
		assert.Len(t, ar.Timestamps, 200)
		assert.Equal(t, exp, ar.Timestamps)
	})
}

// FloatArray attaches the methods of sort.Interface to *tsdb.FloatArray, sorting in increasing order.
type FloatArray struct {
	*cursors.FloatArray
}

func (a *FloatArray) Less(i, j int) bool { return a.Timestamps[i] < a.Timestamps[j] }
func (a *FloatArray) Swap(i, j int) {
	a.Timestamps[i], a.Timestamps[j] = a.Timestamps[j], a.Timestamps[i]
	a.Values[i], a.Values[j] = a.Values[j], a.Values[i]
}

// Verifies the array cursors correctly handle merged blocks from KeyCursor which may exceed the
// array cursor's local values buffer, which is initialized to MaxPointsPerBlock elements (1000)
//
// This test creates two TSM files with a significant number of interleaved points in addition
// to a significant number of points in the second file which replace values in the first.
// To verify intersecting data from the second file replaces the first, the values differ,
// so the enumerated results can be compared with the expected output.
func TestFileStore_MergeBlocksLargerThat1000_MultipleBlocksInEachFile(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := NewFileStore(dir)

	// makeVals creates count points starting at ts and incrementing by step
	makeVals := func(ts, count, step int64, v float64) []Value {
		vals := make([]Value, count)
		for i := range vals {
			vals[i] = NewFloatValue(ts, v)
			ts += step
		}
		return vals
	}

	makeArray := func(ts, count, step int64, v float64) *cursors.FloatArray {
		ar := cursors.NewFloatArrayLen(int(count))
		for i := range ar.Timestamps {
			ar.Timestamps[i] = ts
			ar.Values[i] = v
			ts += step
		}
		return ar
	}

	// Setup 2 files with partially overlapping blocks and the second file replaces some elements of the first
	data := []keyValues{
		{"m,_field=v#!~#v", makeVals(1000, 3500, 10, 1.01)},
		{"m,_field=v#!~#v", makeVals(4005, 3500, 5, 2.01)},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	_ = fs.Replace(nil, files)

	t.Run("ascending", func(t *testing.T) {
		const START, END = 1000, 1e9
		kc := fs.KeyCursor(context.Background(), []byte("m,_field=v#!~#v"), START, true)
		defer kc.Close()
		cur := newFloatArrayAscendingCursor()
		cur.reset(START, END, nil, kc)

		exp := makeArray(1000, 3500, 10, 1.01)
		a2 := makeArray(4005, 3500, 5, 2.01)
		exp.Merge(a2)

		got := cursors.NewFloatArrayLen(exp.Len())
		got.Timestamps = got.Timestamps[:0]
		got.Values = got.Values[:0]

		ar := cur.Next()
		for ar.Len() > 0 {
			got.Timestamps = append(got.Timestamps, ar.Timestamps...)
			got.Values = append(got.Values, ar.Values...)
			ar = cur.Next()
		}

		assert.Len(t, got.Timestamps, exp.Len())
		assert.Equal(t, exp.Timestamps, got.Timestamps)
		assert.Equal(t, exp.Values, got.Values)
	})

	t.Run("descending", func(t *testing.T) {
		const START, END = 1e9, 0
		kc := fs.KeyCursor(context.Background(), []byte("m,_field=v#!~#v"), START, false)
		defer kc.Close()
		cur := newFloatArrayDescendingCursor()
		cur.reset(START, END, nil, kc)

		exp := makeArray(1000, 3500, 10, 1.01)
		a2 := makeArray(4005, 3500, 5, 2.01)
		exp.Merge(a2)
		sort.Sort(sort.Reverse(&FloatArray{exp}))

		got := cursors.NewFloatArrayLen(exp.Len())
		got.Timestamps = got.Timestamps[:0]
		got.Values = got.Values[:0]

		ar := cur.Next()
		for ar.Len() > 0 {
			got.Timestamps = append(got.Timestamps, ar.Timestamps...)
			got.Values = append(got.Values, ar.Values...)
			ar = cur.Next()
		}

		assert.Len(t, got.Timestamps, exp.Len())
		assert.Equal(t, exp.Timestamps, got.Timestamps)
		assert.Equal(t, exp.Values, got.Values)
	})
}

func TestFileStore_SeekBoundaries(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := NewFileStore(dir)

	// makeVals creates count points starting at ts and incrementing by step
	makeVals := func(ts, count, step int64, v float64) []Value {
		vals := make([]Value, count)
		for i := range vals {
			vals[i] = NewFloatValue(ts, v)
			ts += step
		}
		return vals
	}

	makeArray := func(ts, count, step int64, v float64) *cursors.FloatArray {
		ar := cursors.NewFloatArrayLen(int(count))
		for i := range ar.Timestamps {
			ar.Timestamps[i] = ts
			ar.Values[i] = v
			ts += step
		}
		return ar
	}

	// Setup 2 files where the seek time matches the end time.
	data := []keyValues{
		{"m,_field=v#!~#v", makeVals(1000, 100, 1, 1.01)},
		{"m,_field=v#!~#v", makeVals(1100, 100, 1, 2.01)},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %s", err)
	}

	_ = fs.Replace(nil, files)

	t.Run("ascending full", func(t *testing.T) {
		const START, END = 1000, 1099
		kc := fs.KeyCursor(context.Background(), []byte("m,_field=v#!~#v"), START, true)
		defer kc.Close()
		cur := newFloatArrayAscendingCursor()
		cur.reset(START, END, nil, kc)

		exp := makeArray(1000, 100, 1, 1.01)

		got := cursors.NewFloatArrayLen(exp.Len())
		got.Timestamps = got.Timestamps[:0]
		got.Values = got.Values[:0]

		ar := cur.Next()
		for ar.Len() > 0 {
			got.Timestamps = append(got.Timestamps, ar.Timestamps...)
			got.Values = append(got.Values, ar.Values...)
			ar = cur.Next()
		}

		assert.Len(t, got.Timestamps, exp.Len())
		assert.Equal(t, exp.Timestamps, got.Timestamps)
		assert.Equal(t, exp.Values, got.Values)
	})

	t.Run("ascending split", func(t *testing.T) {
		const START, END = 1050, 1149
		kc := fs.KeyCursor(context.Background(), []byte("m,_field=v#!~#v"), START, true)
		defer kc.Close()
		cur := newFloatArrayAscendingCursor()
		cur.reset(START, END, nil, kc)

		exp := makeArray(1050, 50, 1, 1.01)
		a2 := makeArray(1100, 50, 1, 2.01)
		exp.Merge(a2)

		got := cursors.NewFloatArrayLen(exp.Len())
		got.Timestamps = got.Timestamps[:0]
		got.Values = got.Values[:0]

		ar := cur.Next()
		for ar.Len() > 0 {
			got.Timestamps = append(got.Timestamps, ar.Timestamps...)
			got.Values = append(got.Values, ar.Values...)
			ar = cur.Next()
		}

		assert.Len(t, got.Timestamps, exp.Len())
		assert.Equal(t, exp.Timestamps, got.Timestamps)
		assert.Equal(t, exp.Values, got.Values)
	})

	t.Run("descending full", func(t *testing.T) {
		const START, END = 1099, 1000
		kc := fs.KeyCursor(context.Background(), []byte("m,_field=v#!~#v"), START, false)
		defer kc.Close()
		cur := newFloatArrayDescendingCursor()
		cur.reset(START, END, nil, kc)

		exp := makeArray(1000, 100, 1, 1.01)
		sort.Sort(sort.Reverse(&FloatArray{exp}))

		got := cursors.NewFloatArrayLen(exp.Len())
		got.Timestamps = got.Timestamps[:0]
		got.Values = got.Values[:0]

		ar := cur.Next()
		for ar.Len() > 0 {
			got.Timestamps = append(got.Timestamps, ar.Timestamps...)
			got.Values = append(got.Values, ar.Values...)
			ar = cur.Next()
		}

		assert.Len(t, got.Timestamps, exp.Len())
		assert.Equal(t, exp.Timestamps, got.Timestamps)
		assert.Equal(t, exp.Values, got.Values)
	})

	t.Run("descending split", func(t *testing.T) {
		const START, END = 1149, 1050
		kc := fs.KeyCursor(context.Background(), []byte("m,_field=v#!~#v"), START, false)
		defer kc.Close()
		cur := newFloatArrayDescendingCursor()
		cur.reset(START, END, nil, kc)

		exp := makeArray(1050, 50, 1, 1.01)
		a2 := makeArray(1100, 50, 1, 2.01)
		exp.Merge(a2)
		sort.Sort(sort.Reverse(&FloatArray{exp}))

		got := cursors.NewFloatArrayLen(exp.Len())
		got.Timestamps = got.Timestamps[:0]
		got.Values = got.Values[:0]

		ar := cur.Next()
		for ar.Len() > 0 {
			got.Timestamps = append(got.Timestamps, ar.Timestamps...)
			got.Values = append(got.Values, ar.Values...)
			ar = cur.Next()
		}

		assert.Len(t, got.Timestamps, exp.Len())
		assert.Equal(t, exp.Timestamps, got.Timestamps)
		assert.Equal(t, exp.Values, got.Values)
	})
}
