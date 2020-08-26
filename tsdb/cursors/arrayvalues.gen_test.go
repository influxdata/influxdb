package cursors

import (
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func makeIntegerArray(count int, min, max int64) *IntegerArray {
	vals := NewIntegerArrayLen(count)

	ts := min
	inc := (max - min) / int64(count)

	for i := 0; i < count; i++ {
		vals.Timestamps[i] = ts
		ts += inc
	}

	return vals
}

func makeIntegerArrayFromSlice(t []int64) *IntegerArray {
	iv := NewIntegerArrayLen(len(t))
	copy(iv.Timestamps, t)
	return iv
}

func TestIntegerArray_FindRangeNoValues(t *testing.T) {
	var vals IntegerArray
	l, r := vals.FindRange(0, 100)
	if exp := -1; l != exp {
		t.Errorf("invalid l; exp=%d, got=%d", exp, l)
	}
	if exp := -1; r != exp {
		t.Errorf("invalid r; exp=%d, got=%d", exp, r)
	}
}

func TestIntegerArray_FindRange(t *testing.T) {
	vals := makeIntegerArrayFromSlice([]int64{10, 11, 13, 15, 17, 20, 21})

	cases := []struct {
		min, max int64
		l, r     int
	}{
		{12, 20, 2, 5},
		{22, 40, -1, -1},
		{1, 9, -1, -1},
		{1, 10, 0, 0},
		{1, 11, 0, 1},
		{15, 15, 3, 3},
	}

	for _, tc := range cases {
		t.Run(fmt.Sprintf("%d→%d", tc.min, tc.max), func(t *testing.T) {
			l, r := vals.FindRange(tc.min, tc.max)
			if l != tc.l {
				t.Errorf("left: got %d, exp %d", l, tc.l)
			}
			if r != tc.r {
				t.Errorf("right: got %d, exp %d", r, tc.r)
			}
		})
	}
}

func TestIntegerArray_Exclude(t *testing.T) {
	cases := []struct {
		n        string
		min, max int64
		exp      []int64
	}{
		{"excl bad range", 18, 11, []int64{10, 12, 14, 16, 18}},
		{"excl none-lo", 0, 9, []int64{10, 12, 14, 16, 18}},
		{"excl none-hi", 19, 30, []int64{10, 12, 14, 16, 18}},
		{"excl first", 0, 10, []int64{12, 14, 16, 18}},
		{"excl last", 18, 20, []int64{10, 12, 14, 16}},
		{"excl all but first and last", 12, 16, []int64{10, 18}},
		{"excl none in middle", 13, 13, []int64{10, 12, 14, 16, 18}},
		{"excl middle", 14, 14, []int64{10, 12, 16, 18}},
		{"excl suffix", 14, 18, []int64{10, 12}},
	}

	for _, tc := range cases {
		t.Run(fmt.Sprintf("%s[%d,%d]", tc.n, tc.min, tc.max), func(t *testing.T) {
			vals := makeIntegerArray(5, 10, 20)
			vals.Exclude(tc.min, tc.max)
			got := vals.Timestamps
			if !cmp.Equal(got, tc.exp) {
				t.Errorf("unexpected values -got/+exp\n%s", cmp.Diff(got, tc.exp))
			}
		})
	}
}

func TestIntegerArray_Include(t *testing.T) {
	cases := []struct {
		n        string
		min, max int64
		exp      []int64
	}{
		{"incl none-lo", 0, 9, []int64{}},
		{"incl none-hi", 19, 30, []int64{}},
		{"incl first", 0, 10, []int64{10}},
		{"incl last", 18, 20, []int64{18}},
		{"incl all but first and last", 12, 16, []int64{12, 14, 16}},
		{"incl none in middle", 13, 13, []int64{}},
		{"incl middle", 14, 14, []int64{14}},
	}

	for _, tc := range cases {
		t.Run(fmt.Sprintf("%s[%d,%d]", tc.n, tc.min, tc.max), func(t *testing.T) {
			vals := makeIntegerArray(5, 10, 20)
			vals.Include(tc.min, tc.max)
			got := vals.Timestamps
			if !cmp.Equal(got, tc.exp) {
				t.Errorf("unexpected values -got/+exp\n%s", cmp.Diff(got, tc.exp))
			}
		})
	}
}

func makeTimestampArray(count int, min, max int64) *TimestampArray {
	vals := NewTimestampArrayLen(count)

	ts := min
	inc := (max - min) / int64(count)

	for i := 0; i < count; i++ {
		vals.Timestamps[i] = ts
		ts += inc
	}

	return vals
}

func TestTimestampArray_Contains(t *testing.T) {
	cases := []struct {
		n        string
		min, max int64
		exp      bool
	}{
		{"no/lo", 0, 9, false},
		{"no/hi", 19, 30, false},
		{"no/middle", 13, 13, false},

		{"yes/first", 0, 10, true},
		{"yes/first-eq", 10, 10, true},
		{"yes/last", 18, 20, true},
		{"yes/last-eq", 18, 18, true},
		{"yes/all but first and last", 12, 16, true},
		{"yes/middle-eq", 14, 14, true},
		{"yes/middle-overlap", 13, 15, true},
		{"yes/covers", 8, 22, true},
	}

	for _, tc := range cases {
		t.Run(fmt.Sprintf("%s[%d,%d]", tc.n, tc.min, tc.max), func(t *testing.T) {
			vals := makeTimestampArray(5, 10, 20)
			if got := vals.Contains(tc.min, tc.max); got != tc.exp {
				t.Errorf("Contains -got/+exp\n%s", cmp.Diff(got, tc.exp))
			}
		})
	}
}

func benchExclude(b *testing.B, vals *IntegerArray, min, max int64) {
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		vals.Exclude(min, max)
	}
}

func BenchmarkIntegerArray_ExcludeNone_1000(b *testing.B) {
	benchExclude(b, makeIntegerArray(1000, 1000, 2000), 0, 500)
}

func BenchmarkIntegerArray_ExcludeMiddleHalf_1000(b *testing.B) {
	benchExclude(b, makeIntegerArray(1000, 1000, 2000), 1250, 1750)
}

func BenchmarkIntegerArray_ExcludeFirst_1000(b *testing.B) {
	benchExclude(b, makeIntegerArray(1000, 1000, 2000), 0, 1000)
}

func BenchmarkIntegerArray_ExcludeLast_1000(b *testing.B) {
	benchExclude(b, makeIntegerArray(1000, 1000, 2000), 1999, 2000)
}

func BenchmarkIntegerArray_ExcludeNone_10000(b *testing.B) {
	benchExclude(b, makeIntegerArray(10000, 10000, 20000), 00, 5000)
}

func BenchmarkIntegerArray_ExcludeMiddleHalf_10000(b *testing.B) {
	benchExclude(b, makeIntegerArray(10000, 10000, 20000), 12500, 17500)
}

func BenchmarkIntegerArray_ExcludeFirst_10000(b *testing.B) {
	benchExclude(b, makeIntegerArray(10000, 10000, 20000), 0, 10000)
}

func BenchmarkIntegerArray_ExcludeLast_10000(b *testing.B) {
	benchExclude(b, makeIntegerArray(10000, 10000, 20000), 19999, 20000)
}

func benchInclude(b *testing.B, vals *IntegerArray, min, max int64) {
	src := *vals
	tmp := NewIntegerArrayLen(vals.Len())
	copy(tmp.Timestamps, vals.Timestamps)
	copy(tmp.Values, vals.Values)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		vals.Include(min, max)
		*vals = src
		copy(vals.Timestamps, tmp.Timestamps)
		copy(vals.Values, tmp.Values)
	}
}

func BenchmarkIntegerArray_IncludeNone_1000(b *testing.B) {
	benchInclude(b, makeIntegerArray(1000, 1000, 2000), 0, 500)
}

func BenchmarkIntegerArray_IncludeMiddleHalf_1000(b *testing.B) {
	benchInclude(b, makeIntegerArray(1000, 1000, 2000), 1250, 1750)
}

func BenchmarkIntegerArray_IncludeFirst_1000(b *testing.B) {
	benchInclude(b, makeIntegerArray(1000, 1000, 2000), 0, 1000)
}

func BenchmarkIntegerArray_IncludeLast_1000(b *testing.B) {
	benchInclude(b, makeIntegerArray(1000, 1000, 2000), 1999, 2000)
}

func BenchmarkIntegerArray_IncludeNone_10000(b *testing.B) {
	benchInclude(b, makeIntegerArray(10000, 10000, 20000), 00, 5000)
}

func BenchmarkIntegerArray_IncludeMiddleHalf_10000(b *testing.B) {
	benchInclude(b, makeIntegerArray(10000, 10000, 20000), 12500, 17500)
}

func BenchmarkIntegerArray_IncludeFirst_10000(b *testing.B) {
	benchInclude(b, makeIntegerArray(10000, 10000, 20000), 0, 10000)
}

func BenchmarkIntegerArray_IncludeLast_10000(b *testing.B) {
	benchInclude(b, makeIntegerArray(10000, 10000, 20000), 19999, 20000)
}
