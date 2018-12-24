package tsm1

import (
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func makeIntegerValues(count int, min, max int64) IntegerValues {
	vals := make(IntegerValues, count)

	ts := min
	inc := (max - min) / int64(count)

	for i := 0; i < count; i++ {
		vals[i].unixnano = ts
		ts += inc
	}

	return vals
}

func makeIntegerValuesFromSlice(t []int64) IntegerValues {
	iv := make(IntegerValues, len(t))
	for i, v := range t {
		iv[i].unixnano = v
	}
	return iv
}

func TestIntegerValues_FindRangeNoValues(t *testing.T) {
	var vals IntegerValues
	l, r := vals.FindRange(0, 100)
	if exp := -1; l != exp {
		t.Errorf("invalid l; exp=%d, got=%d", exp, l)
	}
	if exp := -1; r != exp {
		t.Errorf("invalid r; exp=%d, got=%d", exp, r)
	}
}

func TestIntegerValues_FindRange(t *testing.T) {
	vals := makeIntegerValuesFromSlice([]int64{10, 11, 13, 15, 17, 20, 21})

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

func TestIntegerValues_Exclude(t *testing.T) {
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
		{"excl suffix", 16, 18, []int64{10, 12, 14}},
	}

	for _, tc := range cases {
		t.Run(fmt.Sprintf("%s[%d,%d]", tc.n, tc.min, tc.max), func(t *testing.T) {
			vals := makeIntegerValues(5, 10, 20)
			vals = vals.Exclude(tc.min, tc.max)
			var got []int64
			for _, v := range vals {
				got = append(got, v.unixnano)
			}
			opt := cmp.AllowUnexported(IntegerValue{})
			if !cmp.Equal(tc.exp, got, opt) {
				t.Error(cmp.Diff(tc.exp, got, opt))
			}
		})
	}
}

func TestIntegerValues_Include(t *testing.T) {
	cases := []struct {
		n        string
		min, max int64
		exp      []int64
	}{
		{"incl none-lo", 0, 9, nil},
		{"incl none-hi", 19, 30, nil},
		{"incl first", 0, 10, []int64{10}},
		{"incl last", 18, 20, []int64{18}},
		{"incl all but first and last", 12, 16, []int64{12, 14, 16}},
		{"incl none in middle", 13, 13, nil},
		{"incl middle", 14, 14, []int64{14}},
	}

	for _, tc := range cases {
		t.Run(fmt.Sprintf("%s[%d,%d]", tc.n, tc.min, tc.max), func(t *testing.T) {
			vals := makeIntegerValues(5, 10, 20)
			vals = vals.Include(tc.min, tc.max)
			var got []int64
			for _, v := range vals {
				got = append(got, v.unixnano)
			}
			opt := cmp.AllowUnexported(IntegerValue{})
			if !cmp.Equal(tc.exp, got, opt) {
				t.Error(cmp.Diff(tc.exp, got, opt))
			}
		})
	}
}

func benchExclude(b *testing.B, vals IntegerValues, min, max int64) {
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		vals.Exclude(min, max)
	}
}

func BenchmarkIntegerValues_ExcludeNone_1000(b *testing.B) {
	benchExclude(b, makeIntegerValues(1000, 1000, 2000), 0, 500)
}

func BenchmarkIntegerValues_ExcludeMiddleHalf_1000(b *testing.B) {
	benchExclude(b, makeIntegerValues(1000, 1000, 2000), 1250, 1750)
}

func BenchmarkIntegerValues_ExcludeFirst_1000(b *testing.B) {
	benchExclude(b, makeIntegerValues(1000, 1000, 2000), 0, 1000)
}

func BenchmarkIntegerValues_ExcludeLast_1000(b *testing.B) {
	benchExclude(b, makeIntegerValues(1000, 1000, 2000), 1999, 2000)
}

func BenchmarkIntegerValues_ExcludeNone_10000(b *testing.B) {
	benchExclude(b, makeIntegerValues(10000, 10000, 20000), 00, 5000)
}

func BenchmarkIntegerValues_ExcludeMiddleHalf_10000(b *testing.B) {
	benchExclude(b, makeIntegerValues(10000, 10000, 20000), 12500, 17500)
}

func BenchmarkIntegerValues_ExcludeFirst_10000(b *testing.B) {
	benchExclude(b, makeIntegerValues(10000, 10000, 20000), 0, 10000)
}

func BenchmarkIntegerValues_ExcludeLast_10000(b *testing.B) {
	benchExclude(b, makeIntegerValues(10000, 10000, 20000), 19999, 20000)
}

func benchInclude(b *testing.B, vals IntegerValues, min, max int64) {
	tmp := append(IntegerValues{}, vals...)
	n := len(vals)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		vals.Include(min, max)
		vals = vals[:n]
		copy(vals, tmp)
	}
}

func BenchmarkIntegerValues_IncludeNone_1000(b *testing.B) {
	benchInclude(b, makeIntegerValues(1000, 1000, 2000), 0, 500)
}

func BenchmarkIntegerValues_IncludeMiddleHalf_1000(b *testing.B) {
	benchInclude(b, makeIntegerValues(1000, 1000, 2000), 1250, 1750)
}

func BenchmarkIntegerValues_IncludeFirst_1000(b *testing.B) {
	benchInclude(b, makeIntegerValues(1000, 1000, 2000), 0, 1000)
}

func BenchmarkIntegerValues_IncludeLast_1000(b *testing.B) {
	benchInclude(b, makeIntegerValues(1000, 1000, 2000), 1999, 2000)
}

func BenchmarkIntegerValues_IncludeNone_10000(b *testing.B) {
	benchInclude(b, makeIntegerValues(10000, 10000, 20000), 00, 5000)
}

func BenchmarkIntegerValues_IncludeMiddleHalf_10000(b *testing.B) {
	benchInclude(b, makeIntegerValues(10000, 10000, 20000), 12500, 17500)
}

func BenchmarkIntegerValues_IncludeFirst_10000(b *testing.B) {
	benchInclude(b, makeIntegerValues(10000, 10000, 20000), 0, 10000)
}

func BenchmarkIntegerValues_IncludeLast_10000(b *testing.B) {
	benchInclude(b, makeIntegerValues(10000, 10000, 20000), 19999, 20000)
}
