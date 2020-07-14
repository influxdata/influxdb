package reads

import (
	"math"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/v2/tsdb/cursors"
)

func TestIntegerFilterArrayCursor(t *testing.T) {
	var i int
	expr := MockExpression{
		EvalBoolFunc: func(v Valuer) bool {
			i++
			return i%2 == 0
		},
	}

	var resultN int
	ac := MockIntegerArrayCursor{
		CloseFunc: func() {},
		ErrFunc:   func() error { return nil },
		StatsFunc: func() cursors.CursorStats { return cursors.CursorStats{} },
		NextFunc: func() *cursors.IntegerArray {
			resultN++
			if resultN == 4 {
				return cursors.NewIntegerArrayLen(0)
			}
			return cursors.NewIntegerArrayLen(900)
		},
	}

	c := newIntegerFilterArrayCursor(&expr)
	c.reset(&ac)

	if got, want := len(c.Next().Timestamps), 1000; got != want {
		t.Fatalf("len(Next())=%d, want %d", got, want)
	} else if got, want := len(c.Next().Timestamps), 350; got != want {
		t.Fatalf("len(Next())=%d, want %d", got, want)
	}
}

func makeIntegerArray(n int, tsStart time.Time, tsStep time.Duration, valueFn func(i int64) int64) *cursors.IntegerArray {
	ia := &cursors.IntegerArray{
		Timestamps: make([]int64, n),
		Values:     make([]int64, n),
	}

	for i := 0; i < n; i++ {
		ia.Timestamps[i] = tsStart.UnixNano() + int64(i)*int64(tsStep)
		ia.Values[i] = valueFn(int64(i))
	}

	return ia
}

func mustParseTime(ts string) time.Time {
	t, err := time.Parse(time.RFC3339, ts)
	if err != nil {
		panic(err)
	}
	return t
}

func copyIntegerArray(src *cursors.IntegerArray) *cursors.IntegerArray {
	dst := cursors.NewIntegerArrayLen(src.Len())
	copy(dst.Timestamps, src.Timestamps)
	copy(dst.Values, src.Values)
	return dst
}

type aggArrayCursorTest struct {
	name           string
	createCursorFn func(cur cursors.IntegerArrayCursor, every, offset int64) cursors.IntegerArrayCursor
	every          time.Duration
	offset         time.Duration
	inputArrays    []*cursors.IntegerArray
	want           []*cursors.IntegerArray
}

func (a *aggArrayCursorTest) run(t *testing.T) {
	t.Helper()
	t.Run(a.name, func(t *testing.T) {
		var resultN int
		mc := &MockIntegerArrayCursor{
			CloseFunc: func() {},
			ErrFunc:   func() error { return nil },
			StatsFunc: func() cursors.CursorStats { return cursors.CursorStats{} },
			NextFunc: func() *cursors.IntegerArray {
				if resultN < len(a.inputArrays) {
					a := a.inputArrays[resultN]
					resultN++
					return a
				}
				return &cursors.IntegerArray{}
			},
		}
		cursor := a.createCursorFn(mc, int64(a.every), int64(a.offset))
		got := make([]*cursors.IntegerArray, 0, len(a.want))
		for a := cursor.Next(); a.Len() != 0; a = cursor.Next() {
			got = append(got, copyIntegerArray(a))
		}

		if diff := cmp.Diff(got, a.want); diff != "" {
			t.Fatalf("did not get expected result from count array cursor; -got/+want:\n%v", diff)
		}

	})
}

func TestLimitArrayCursor(t *testing.T) {
	arr := []*cursors.IntegerArray{
		makeIntegerArray(
			1000,
			mustParseTime("1970-01-01T00:00:01Z"), time.Millisecond,
			func(i int64) int64 { return 3 + i },
		),
		makeIntegerArray(
			1000,
			mustParseTime("1970-01-01T00:00:02Z"), time.Millisecond,
			func(i int64) int64 { return 1003 + i },
		),
	}
	idx := -1
	cur := &MockIntegerArrayCursor{
		CloseFunc: func() {},
		ErrFunc:   func() error { return nil },
		StatsFunc: func() cursors.CursorStats { return cursors.CursorStats{} },
		NextFunc: func() *cursors.IntegerArray {
			if idx++; idx < len(arr) {
				return arr[idx]
			}
			return &cursors.IntegerArray{}
		},
	}
	aggCursor := newIntegerLimitArrayCursor(cur)
	want := []*cursors.IntegerArray{
		{
			Timestamps: []int64{mustParseTime("1970-01-01T00:00:01Z").UnixNano()},
			Values:     []int64{3},
		},
	}
	got := []*cursors.IntegerArray{}
	for a := aggCursor.Next(); a.Len() != 0; a = aggCursor.Next() {
		got = append(got, a)
	}
	if !cmp.Equal(want, got) {
		t.Fatalf("unexpected result; -want/+got:\n%v", cmp.Diff(want, got))
	}
}

func TestWindowFirstArrayCursor(t *testing.T) {
	testcases := []aggArrayCursorTest{
		{
			name:  "window",
			every: 15 * time.Minute,
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray(
					60,
					mustParseTime("2010-01-01T00:00:00Z"), time.Minute,
					func(i int64) int64 { return i },
				),
			},
			want: []*cursors.IntegerArray{
				makeIntegerArray(
					4,
					mustParseTime("2010-01-01T00:00:00Z"), 15*time.Minute,
					func(i int64) int64 { return 15 * i },
				),
			},
		},
		{
			name:   "offset window",
			every:  15 * time.Minute,
			offset: time.Minute,
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray(
					60,
					mustParseTime("2010-01-01T00:00:00Z"), time.Minute,
					func(i int64) int64 { return i },
				),
			},
			want: []*cursors.IntegerArray{
				{
					Timestamps: []int64{
						mustParseTime("2010-01-01T00:00:00Z").UnixNano(),
						mustParseTime("2010-01-01T00:01:00Z").UnixNano(),
						mustParseTime("2010-01-01T00:16:00Z").UnixNano(),
						mustParseTime("2010-01-01T00:31:00Z").UnixNano(),
						mustParseTime("2010-01-01T00:46:00Z").UnixNano(),
					},
					Values: []int64{0, 1, 16, 31, 46},
				},
			},
		},
		{
			name:  "empty windows",
			every: time.Minute,
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray(
					4,
					mustParseTime("2010-01-01T00:00:00Z"), 15*time.Minute,
					func(i int64) int64 { return i },
				),
			},
			want: []*cursors.IntegerArray{
				makeIntegerArray(
					4,
					mustParseTime("2010-01-01T00:00:00Z"), 15*time.Minute,
					func(i int64) int64 { return i },
				),
			},
		},
		{
			name:   "empty offset windows",
			every:  time.Minute,
			offset: time.Minute,
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray(
					4,
					mustParseTime("2010-01-01T00:00:00Z"), 15*time.Minute,
					func(i int64) int64 { return i },
				),
			},
			want: []*cursors.IntegerArray{
				makeIntegerArray(
					4,
					mustParseTime("2010-01-01T00:00:00Z"), 15*time.Minute,
					func(i int64) int64 { return i },
				),
			},
		},
		{
			name:  "unaligned window",
			every: 15 * time.Minute,
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray(
					60,
					mustParseTime("2010-01-01T00:00:30Z"), time.Minute,
					func(i int64) int64 { return i },
				),
			},
			want: []*cursors.IntegerArray{
				makeIntegerArray(
					4,
					mustParseTime("2010-01-01T00:00:30Z"), 15*time.Minute,
					func(i int64) int64 { return 15 * i },
				),
			},
		},
		{
			name:   "unaligned offset window",
			every:  15 * time.Minute,
			offset: 45 * time.Second,
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray(
					60,
					mustParseTime("2010-01-01T00:00:30Z"), time.Minute,
					func(i int64) int64 { return i },
				),
			},
			want: []*cursors.IntegerArray{
				{
					Timestamps: []int64{
						mustParseTime("2010-01-01T00:00:30Z").UnixNano(),
						mustParseTime("2010-01-01T00:01:30Z").UnixNano(),
						mustParseTime("2010-01-01T00:16:30Z").UnixNano(),
						mustParseTime("2010-01-01T00:31:30Z").UnixNano(),
						mustParseTime("2010-01-01T00:46:30Z").UnixNano(),
					},
					Values: []int64{0, 1, 16, 31, 46},
				},
			},
		},
		{
			name:  "more unaligned window",
			every: 15 * time.Minute,
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray(
					60,
					mustParseTime("2010-01-01T00:01:30Z"), time.Minute,
					func(i int64) int64 { return i },
				),
			},
			want: []*cursors.IntegerArray{
				{
					Timestamps: []int64{
						mustParseTime("2010-01-01T00:01:30Z").UnixNano(),
						mustParseTime("2010-01-01T00:15:30Z").UnixNano(),
						mustParseTime("2010-01-01T00:30:30Z").UnixNano(),
						mustParseTime("2010-01-01T00:45:30Z").UnixNano(),
						mustParseTime("2010-01-01T01:00:30Z").UnixNano(),
					},
					Values: []int64{0, 14, 29, 44, 59},
				},
			},
		},
		{
			name:  "window two input arrays",
			every: 15 * time.Minute,
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray(
					60,
					mustParseTime("2010-01-01T00:00:00Z"), time.Minute,
					func(i int64) int64 { return i },
				),
				makeIntegerArray(
					60,
					mustParseTime("2010-01-01T01:00:00Z"), time.Minute,
					func(i int64) int64 { return 60 + i },
				),
			},
			want: []*cursors.IntegerArray{
				makeIntegerArray(
					8,
					mustParseTime("2010-01-01T00:00:00Z"), 15*time.Minute,
					func(i int64) int64 { return 15 * i },
				),
			},
		},
		{
			name:   "offset window two input arrays",
			every:  30 * time.Minute,
			offset: 27 * time.Minute,
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray(
					60,
					mustParseTime("2010-01-01T00:00:00Z"), time.Minute,
					func(i int64) int64 { return i },
				),
				makeIntegerArray(
					60,
					mustParseTime("2010-01-01T01:00:00Z"), time.Minute,
					func(i int64) int64 { return 60 + i },
				),
			},
			want: []*cursors.IntegerArray{
				{
					Timestamps: []int64{
						mustParseTime("2010-01-01T00:00:00Z").UnixNano(),
						mustParseTime("2010-01-01T00:27:00Z").UnixNano(),
						mustParseTime("2010-01-01T00:57:00Z").UnixNano(),
						mustParseTime("2010-01-01T01:27:00Z").UnixNano(),
						mustParseTime("2010-01-01T01:57:00Z").UnixNano(),
					},
					Values: []int64{0, 27, 57, 87, 117},
				},
			},
		},
		{
			name:  "window spans input arrays",
			every: 40 * time.Minute,
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray(
					60,
					mustParseTime("2010-01-01T00:00:00Z"), time.Minute,
					func(i int64) int64 { return i },
				),
				makeIntegerArray(
					60,
					mustParseTime("2010-01-01T01:00:00Z"), time.Minute,
					func(i int64) int64 { return 60 + i },
				),
			},
			want: []*cursors.IntegerArray{
				makeIntegerArray(
					3,
					mustParseTime("2010-01-01T00:00:00Z"), 40*time.Minute,
					func(i int64) int64 { return 40 * i },
				),
			},
		},
		{
			name:   "offset window spans input arrays",
			every:  40 * time.Minute,
			offset: 10 * time.Minute,
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray(
					60,
					mustParseTime("2010-01-01T00:00:00Z"), time.Minute,
					func(i int64) int64 { return i },
				),
				makeIntegerArray(
					60,
					mustParseTime("2010-01-01T01:00:00Z"), time.Minute,
					func(i int64) int64 { return 60 + i },
				),
			},
			want: []*cursors.IntegerArray{
				{
					Timestamps: []int64{
						mustParseTime("2010-01-01T00:00:00Z").UnixNano(),
						mustParseTime("2010-01-01T00:10:00Z").UnixNano(),
						mustParseTime("2010-01-01T00:50:00Z").UnixNano(),
						mustParseTime("2010-01-01T01:30:00Z").UnixNano(),
					},
					Values: []int64{0, 10, 50, 90},
				},
			},
		},
		{
			name:  "more windows than MaxPointsPerBlock",
			every: 2 * time.Millisecond,
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray( // 1 second, one point per ms
					1000,
					mustParseTime("2010-01-01T00:00:00Z"), time.Millisecond,
					func(i int64) int64 { return i },
				),
				makeIntegerArray( // 1 second, one point per ms
					1000,
					mustParseTime("2010-01-01T00:00:01Z"), time.Millisecond,
					func(i int64) int64 { return 1000 + i },
				),
				makeIntegerArray( // 1 second, one point per ms
					1000,
					mustParseTime("2010-01-01T00:00:02Z"), time.Millisecond,
					func(i int64) int64 { return 2000 + i },
				),
			},
			want: []*cursors.IntegerArray{
				makeIntegerArray(
					1000,
					mustParseTime("2010-01-01T00:00:00.000Z"), 2*time.Millisecond,
					func(i int64) int64 { return 2 * i },
				),
				makeIntegerArray(
					500,
					mustParseTime("2010-01-01T00:00:02.000Z"), 2*time.Millisecond,
					func(i int64) int64 { return 2000 + 2*i },
				),
			},
		},
		{
			name:   "more offset windows than MaxPointsPerBlock",
			every:  2 * time.Millisecond,
			offset: 1 * time.Millisecond,
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray( // 1 second, one point per ms
					1000,
					mustParseTime("2010-01-01T00:00:00Z"), time.Millisecond,
					func(i int64) int64 { return i },
				),
				makeIntegerArray( // 1 second, one point per ms
					1000,
					mustParseTime("2010-01-01T00:00:01Z"), time.Millisecond,
					func(i int64) int64 { return 1000 + i },
				),
				makeIntegerArray( // 1 second, one point per ms
					1000,
					mustParseTime("2010-01-01T00:00:02Z"), time.Millisecond,
					func(i int64) int64 { return 2000 + i },
				),
			},
			want: []*cursors.IntegerArray{
				func() *cursors.IntegerArray {
					arr := makeIntegerArray(
						999,
						mustParseTime("2010-01-01T00:00:00.001Z"), 2*time.Millisecond,
						func(i int64) int64 { return 1 + 2*i },
					)
					return &cursors.IntegerArray{
						Timestamps: append([]int64{mustParseTime("2010-01-01T00:00:00.000Z").UnixNano()}, arr.Timestamps...),
						Values:     append([]int64{0}, arr.Values...),
					}
				}(),
				makeIntegerArray(
					501,
					mustParseTime("2010-01-01T00:00:01.999Z"), 2*time.Millisecond,
					func(i int64) int64 { return 1999 + 2*i },
				),
			},
		},
		{
			name: "whole series",
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray(
					60,
					mustParseTime("2010-01-01T00:00:00Z"), time.Minute,
					func(i int64) int64 { return 100 + i },
				),
			},
			want: []*cursors.IntegerArray{
				makeIntegerArray(
					1,
					mustParseTime("2010-01-01T00:00:00Z"), time.Minute,
					func(int64) int64 { return 100 },
				),
			},
		},
		{
			name:        "whole series no points",
			inputArrays: []*cursors.IntegerArray{{}},
			want:        []*cursors.IntegerArray{},
		},
		{
			name: "whole series two arrays",
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray(
					60,
					mustParseTime("2010-01-01T00:00:00Z"), time.Minute,
					func(i int64) int64 { return 10 + i },
				),
				makeIntegerArray(
					60,
					mustParseTime("2010-01-01T01:00:00Z"), time.Minute,
					func(i int64) int64 { return 70 + i },
				),
			},
			want: []*cursors.IntegerArray{
				makeIntegerArray(
					1,
					mustParseTime("2010-01-01T00:00:00Z"), time.Minute,
					func(int64) int64 { return 10 },
				),
			},
		},
		{
			name: "whole series span epoch",
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray(
					120,
					mustParseTime("1969-12-31T23:00:00Z"), time.Minute,
					func(i int64) int64 { return 100 + i },
				),
			},
			want: []*cursors.IntegerArray{
				makeIntegerArray(
					1,
					mustParseTime("1969-12-31T23:00:00Z"), time.Minute,
					func(int64) int64 { return 100 },
				),
			},
		},
		{
			name: "whole series span epoch two arrays",
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray(
					60,
					mustParseTime("1969-12-31T23:00:00Z"), time.Minute,
					func(i int64) int64 { return 100 + i },
				),
				makeIntegerArray(
					60,
					mustParseTime("1970-01-01T00:00:00Z"), time.Minute,
					func(i int64) int64 { return 160 + i },
				),
			},
			want: []*cursors.IntegerArray{
				makeIntegerArray(
					1,
					mustParseTime("1969-12-31T23:00:00Z"), time.Minute,
					func(int64) int64 { return 100 },
				),
			},
		},
		{
			name: "whole series, with max int64 timestamp",
			inputArrays: []*cursors.IntegerArray{
				{
					Timestamps: []int64{math.MaxInt64},
					Values:     []int64{12},
				},
			},
			want: []*cursors.IntegerArray{
				{
					Timestamps: []int64{math.MaxInt64},
					Values:     []int64{12},
				},
			},
		},
	}
	for _, tc := range testcases {
		tc.createCursorFn = func(cur cursors.IntegerArrayCursor, every, offset int64) cursors.IntegerArrayCursor {
			if every == 0 {
				return newIntegerLimitArrayCursor(cur)
			}
			return newIntegerWindowFirstArrayCursor(cur, every, offset)
		}
		tc.run(t)
	}
}

func TestWindowLastArrayCursor(t *testing.T) {
	testcases := []aggArrayCursorTest{
		{
			name:  "window",
			every: 15 * time.Minute,
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray(
					60,
					mustParseTime("2010-01-01T00:00:00Z"), time.Minute,
					func(i int64) int64 { return i },
				),
			},
			want: []*cursors.IntegerArray{
				makeIntegerArray(
					4,
					mustParseTime("2010-01-01T00:14:00Z"), 15*time.Minute,
					func(i int64) int64 { return 14 + 15*i },
				),
			},
		},
		{
			name:  "empty windows",
			every: time.Minute,
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray(
					4,
					mustParseTime("2010-01-01T00:00:00Z"), 15*time.Minute,
					func(i int64) int64 { return i },
				),
			},
			want: []*cursors.IntegerArray{
				makeIntegerArray(
					4,
					mustParseTime("2010-01-01T00:00:00Z"), 15*time.Minute,
					func(i int64) int64 { return i },
				),
			},
		},
		{
			name:  "unaligned window",
			every: 15 * time.Minute,
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray(
					60,
					mustParseTime("2010-01-01T00:00:30Z"), time.Minute,
					func(i int64) int64 { return i },
				),
			},
			want: []*cursors.IntegerArray{
				makeIntegerArray(
					4,
					mustParseTime("2010-01-01T00:14:30Z"), 15*time.Minute,
					func(i int64) int64 { return 14 + 15*i },
				),
			},
		},
		{
			name:  "more unaligned window",
			every: 15 * time.Minute,
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray(
					60,
					mustParseTime("2010-01-01T00:01:30Z"), time.Minute,
					func(i int64) int64 { return i },
				),
			},
			want: []*cursors.IntegerArray{
				{
					Timestamps: []int64{
						mustParseTime("2010-01-01T00:14:30Z").UnixNano(),
						mustParseTime("2010-01-01T00:29:30Z").UnixNano(),
						mustParseTime("2010-01-01T00:44:30Z").UnixNano(),
						mustParseTime("2010-01-01T00:59:30Z").UnixNano(),
						mustParseTime("2010-01-01T01:00:30Z").UnixNano(),
					},
					Values: []int64{13, 28, 43, 58, 59},
				},
			},
		},
		{
			name:  "window two input arrays",
			every: 15 * time.Minute,
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray(
					60,
					mustParseTime("2010-01-01T00:00:00Z"), time.Minute,
					func(i int64) int64 { return i },
				),
				makeIntegerArray(
					60,
					mustParseTime("2010-01-01T01:00:00Z"), time.Minute,
					func(i int64) int64 { return 60 + i },
				),
			},
			want: []*cursors.IntegerArray{
				makeIntegerArray(
					8,
					mustParseTime("2010-01-01T00:14:00Z"), 15*time.Minute,
					func(i int64) int64 { return 14 + 15*i },
				),
			},
		},
		{
			name:  "window spans input arrays",
			every: 40 * time.Minute,
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray(
					60,
					mustParseTime("2010-01-01T00:00:00Z"), time.Minute,
					func(i int64) int64 { return i },
				),
				makeIntegerArray(
					60,
					mustParseTime("2010-01-01T01:00:00Z"), time.Minute,
					func(i int64) int64 { return 60 + i },
				),
			},
			want: []*cursors.IntegerArray{
				makeIntegerArray(
					3,
					mustParseTime("2010-01-01T00:39:00Z"), 40*time.Minute,
					func(i int64) int64 { return 39 + 40*i },
				),
			},
		},
		{
			name:  "more windows than MaxPointsPerBlock",
			every: 2 * time.Millisecond,
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray( // 1 second, one point per ms
					1000,
					mustParseTime("2010-01-01T00:00:00Z"), time.Millisecond,
					func(i int64) int64 { return i },
				),
				makeIntegerArray( // 1 second, one point per ms
					1000,
					mustParseTime("2010-01-01T00:00:01Z"), time.Millisecond,
					func(i int64) int64 { return 1000 + i },
				),
				makeIntegerArray( // 1 second, one point per ms
					1000,
					mustParseTime("2010-01-01T00:00:02Z"), time.Millisecond,
					func(i int64) int64 { return 2000 + i },
				),
			},
			want: []*cursors.IntegerArray{
				makeIntegerArray(
					1000,
					mustParseTime("2010-01-01T00:00:00.001Z"), 2*time.Millisecond,
					func(i int64) int64 { return 1 + 2*i },
				),
				makeIntegerArray(
					500,
					mustParseTime("2010-01-01T00:00:02.001Z"), 2*time.Millisecond,
					func(i int64) int64 { return 2001 + 2*i },
				),
			},
		},
		{
			name:  "MaxPointsPerBlock",
			every: time.Millisecond,
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray( // 1 second, one point per ms
					1000,
					mustParseTime("2010-01-01T00:00:00Z"), time.Millisecond,
					func(i int64) int64 { return i },
				),
				makeIntegerArray( // 1 second, one point per ms
					1000,
					mustParseTime("2010-01-01T00:00:01Z"), time.Millisecond,
					func(i int64) int64 { return 1000 + i },
				),
			},
			want: []*cursors.IntegerArray{
				makeIntegerArray(
					1000,
					mustParseTime("2010-01-01T00:00:00Z"), time.Millisecond,
					func(i int64) int64 { return i },
				),
				makeIntegerArray(
					1000,
					mustParseTime("2010-01-01T00:00:01Z"), time.Millisecond,
					func(i int64) int64 { return 1000 + i },
				),
			},
		},
	}
	for _, tc := range testcases {
		tc.createCursorFn = func(cur cursors.IntegerArrayCursor, every, offset int64) cursors.IntegerArrayCursor {
			return newIntegerWindowLastArrayCursor(cur, every, offset)
		}
		tc.run(t)
	}
}

func TestIntegerCountArrayCursor(t *testing.T) {
	maxTimestamp := time.Unix(0, math.MaxInt64)

	testcases := []aggArrayCursorTest{
		{
			name:  "window",
			every: 15 * time.Minute,
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray(
					60,
					mustParseTime("2010-01-01T00:00:00Z"), time.Minute,
					func(i int64) int64 { return 100 + i },
				),
			},
			want: []*cursors.IntegerArray{
				makeIntegerArray(4, mustParseTime("2010-01-01T00:15:00Z"), 15*time.Minute, func(int64) int64 { return 15 }),
			},
		},
		{
			name:   "offset window",
			every:  15 * time.Minute,
			offset: time.Minute,
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray(
					60,
					mustParseTime("2010-01-01T00:00:00Z"), time.Minute,
					func(i int64) int64 { return i },
				),
			},
			want: []*cursors.IntegerArray{
				makeIntegerArray(5, mustParseTime("2010-01-01T00:01:00Z"), 15*time.Minute, func(i int64) int64 {
					switch i {
					case 0:
						return 1
					case 4:
						return 14
					default:
						return 15
					}
				}),
			},
		},
		{
			name:  "empty windows",
			every: time.Minute,
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray(
					4,
					mustParseTime("2010-01-01T00:00:00Z"), 15*time.Minute,
					func(i int64) int64 { return 100 + i },
				),
			},
			want: []*cursors.IntegerArray{
				makeIntegerArray(
					4,
					mustParseTime("2010-01-01T00:01:00Z"), 15*time.Minute,
					func(i int64) int64 { return 1 },
				),
			},
		},
		{
			name:   "empty offset windows",
			every:  time.Minute,
			offset: time.Minute,
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray(
					4,
					mustParseTime("2010-01-01T00:00:00Z"), 15*time.Minute,
					func(i int64) int64 { return 100 + i },
				),
			},
			want: []*cursors.IntegerArray{
				makeIntegerArray(
					4,
					mustParseTime("2010-01-01T00:01:00Z"), 15*time.Minute,
					func(int64) int64 { return 1 },
				),
			},
		},
		{
			name:  "unaligned window",
			every: 15 * time.Minute,
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray(
					60,
					mustParseTime("2010-01-01T00:00:30Z"), time.Minute,
					func(i int64) int64 { return 100 + i },
				),
			},
			want: []*cursors.IntegerArray{
				makeIntegerArray(
					4,
					mustParseTime("2010-01-01T00:15:00Z"), 15*time.Minute,
					func(i int64) int64 {
						return 15
					}),
			},
		},
		{
			name:   "unaligned offset window",
			every:  15 * time.Minute,
			offset: 45 * time.Second,
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray(
					60,
					mustParseTime("2010-01-01T00:00:30Z"), time.Minute,
					func(i int64) int64 { return i },
				),
			},
			want: []*cursors.IntegerArray{
				makeIntegerArray(
					5,
					mustParseTime("2010-01-01T00:00:45Z"), 15*time.Minute,
					func(i int64) int64 {
						switch i {
						case 0:
							return 1
						case 4:
							return 14
						default:
							return 15
						}
					}),
			},
		},
		{
			name:  "more unaligned window",
			every: 15 * time.Minute,
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray(
					60,
					mustParseTime("2010-01-01T00:01:30Z"), time.Minute,
					func(i int64) int64 { return 100 + i },
				),
			},
			want: []*cursors.IntegerArray{
				makeIntegerArray(
					5,
					mustParseTime("2010-01-01T00:15:00Z"), 15*time.Minute,
					func(i int64) int64 {
						switch i {
						case 0:
							return 14
						case 4:
							return 1
						default:
							return 15
						}
					}),
			},
		},
		{
			name:  "window two input arrays",
			every: 15 * time.Minute,
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray(
					60,
					mustParseTime("2010-01-01T00:00:00Z"), time.Minute,
					func(i int64) int64 { return 100 + i },
				),
				makeIntegerArray(
					60,
					mustParseTime("2010-01-01T01:00:00Z"), time.Minute,
					func(i int64) int64 { return 200 + i },
				),
			},
			want: []*cursors.IntegerArray{
				makeIntegerArray(8, mustParseTime("2010-01-01T00:15:00Z"), 15*time.Minute, func(int64) int64 { return 15 }),
			},
		},
		{
			name:   "offset window two input arrays",
			every:  30 * time.Minute,
			offset: 27 * time.Minute,
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray(
					60,
					mustParseTime("2010-01-01T00:00:00Z"), time.Minute,
					func(i int64) int64 { return i },
				),
				makeIntegerArray(
					60,
					mustParseTime("2010-01-01T01:00:00Z"), time.Minute,
					func(i int64) int64 { return 60 + i },
				),
			},
			want: []*cursors.IntegerArray{
				makeIntegerArray(5, mustParseTime("2010-01-01T00:27:00Z"), 30*time.Minute, func(i int64) int64 {
					switch i {
					case 0:
						return 27
					case 4:
						return 3
					default:
						return 30
					}
				}),
			},
		},
		{
			name:  "window spans input arrays",
			every: 40 * time.Minute,
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray(
					60,
					mustParseTime("2010-01-01T00:00:00Z"), time.Minute,
					func(i int64) int64 { return 100 + i },
				),
				makeIntegerArray(
					60,
					mustParseTime("2010-01-01T01:00:00Z"), time.Minute,
					func(i int64) int64 { return 200 + i },
				),
			},
			want: []*cursors.IntegerArray{
				makeIntegerArray(3, mustParseTime("2010-01-01T00:40:00Z"), 40*time.Minute, func(int64) int64 { return 40 }),
			},
		},
		{
			name:   "offset window spans input arrays",
			every:  40 * time.Minute,
			offset: 10 * time.Minute,
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray(
					60,
					mustParseTime("2010-01-01T00:00:00Z"), time.Minute,
					func(i int64) int64 { return i },
				),
				makeIntegerArray(
					60,
					mustParseTime("2010-01-01T01:00:00Z"), time.Minute,
					func(i int64) int64 { return 60 + i },
				),
			},
			want: []*cursors.IntegerArray{
				makeIntegerArray(4, mustParseTime("2010-01-01T00:10:00Z"), 40*time.Minute, func(i int64) int64 {
					switch i {
					case 0:
						return 10
					case 3:
						return 30
					default:
						return 40
					}
				}),
			},
		},
		{
			name:  "more windows than MaxPointsPerBlock",
			every: 2 * time.Millisecond,
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray( // 1 second, one point per ms
					1000,
					mustParseTime("2010-01-01T00:00:00Z"), time.Millisecond,
					func(i int64) int64 { return i },
				),
				makeIntegerArray( // 1 second, one point per ms
					1000,
					mustParseTime("2010-01-01T00:00:01Z"), time.Millisecond,
					func(i int64) int64 { return i },
				),
				makeIntegerArray( // 1 second, one point per ms
					1000,
					mustParseTime("2010-01-01T00:00:02Z"), time.Millisecond,
					func(i int64) int64 { return i },
				),
			},
			want: []*cursors.IntegerArray{
				makeIntegerArray(
					1000,
					mustParseTime("2010-01-01T00:00:00.002Z"), 2*time.Millisecond,
					func(i int64) int64 { return 2 },
				),
				makeIntegerArray(
					500,
					mustParseTime("2010-01-01T00:00:02.002Z"), 2*time.Millisecond,
					func(i int64) int64 { return 2 },
				),
			},
		},
		{
			name:   "more offset windows than MaxPointsPerBlock",
			every:  2 * time.Millisecond,
			offset: 1 * time.Millisecond,
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray( // 1 second, one point per ms
					1000,
					mustParseTime("2010-01-01T00:00:00Z"), time.Millisecond,
					func(i int64) int64 { return i },
				),
				makeIntegerArray( // 1 second, one point per ms
					1000,
					mustParseTime("2010-01-01T00:00:01Z"), time.Millisecond,
					func(i int64) int64 { return 1000 + i },
				),
				makeIntegerArray( // 1 second, one point per ms
					1000,
					mustParseTime("2010-01-01T00:00:02Z"), time.Millisecond,
					func(i int64) int64 { return 2000 + i },
				),
			},
			want: []*cursors.IntegerArray{
				makeIntegerArray(
					1000,
					mustParseTime("2010-01-01T00:00:00.001Z"), 2*time.Millisecond,
					func(i int64) int64 {
						switch i {
						case 0:
							return 1
						default:
							return 2
						}
					},
				),
				makeIntegerArray(
					501,
					mustParseTime("2010-01-01T00:00:02.001Z"), 2*time.Millisecond,
					func(i int64) int64 {
						switch i {
						case 500:
							return 1
						default:
							return 2
						}
					},
				),
			},
		},
		{
			name: "whole series",
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray(
					60,
					mustParseTime("2010-01-01T00:00:00Z"), time.Minute,
					func(i int64) int64 { return 100 + i },
				),
			},
			want: []*cursors.IntegerArray{
				makeIntegerArray(1, maxTimestamp, 40*time.Minute, func(i int64) int64 { return 60 }),
			},
		},
		{
			name:        "whole series no points",
			inputArrays: []*cursors.IntegerArray{{}},
			want:        []*cursors.IntegerArray{},
		},
		{
			name: "whole series two arrays",
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray(
					60,
					mustParseTime("2010-01-01T00:00:00Z"), time.Minute,
					func(i int64) int64 { return 100 + i },
				),
				makeIntegerArray(
					60,
					mustParseTime("2010-01-01T01:00:00Z"), time.Minute,
					func(i int64) int64 { return 100 + i },
				),
			},
			want: []*cursors.IntegerArray{
				makeIntegerArray(1, maxTimestamp, 40*time.Minute, func(int64) int64 { return 120 }),
			},
		},
		{
			name: "whole series span epoch",
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray(
					120,
					mustParseTime("1969-12-31T23:00:00Z"), time.Minute,
					func(i int64) int64 { return 100 + i },
				),
			},
			want: []*cursors.IntegerArray{
				makeIntegerArray(1, maxTimestamp, 40*time.Minute, func(int64) int64 { return 120 }),
			},
		},
		{
			name: "whole series span epoch two arrays",
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray(
					60,
					mustParseTime("1969-12-31T23:00:00Z"), time.Minute,
					func(i int64) int64 { return 100 + i },
				),
				makeIntegerArray(
					60,
					mustParseTime("1970-01-01T00:00:00Z"), time.Minute,
					func(i int64) int64 { return 100 + i },
				),
			},
			want: []*cursors.IntegerArray{
				makeIntegerArray(1, maxTimestamp, 40*time.Minute, func(int64) int64 { return 120 }),
			},
		},
		{
			name: "whole series, with max int64 timestamp",
			inputArrays: []*cursors.IntegerArray{
				{
					Timestamps: []int64{math.MaxInt64},
					Values:     []int64{0},
				},
			},
			want: []*cursors.IntegerArray{
				{
					Timestamps: []int64{math.MaxInt64},
					Values:     []int64{1},
				},
			},
		},
	}
	for _, tc := range testcases {
		tc.createCursorFn = func(cur cursors.IntegerArrayCursor, every, offset int64) cursors.IntegerArrayCursor {
			return newIntegerWindowCountArrayCursor(cur, every, offset)
		}
		tc.run(t)
	}
}

func TestIntegerSumArrayCursor(t *testing.T) {
	maxTimestamp := time.Unix(0, math.MaxInt64)

	testcases := []aggArrayCursorTest{
		{
			name:  "window",
			every: 15 * time.Minute,
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray(
					60,
					mustParseTime("2010-01-01T00:00:00Z"), time.Minute,
					func(i int64) int64 { return 2 },
				),
			},
			want: []*cursors.IntegerArray{
				makeIntegerArray(4, mustParseTime("2010-01-01T00:15:00Z"), 15*time.Minute, func(int64) int64 { return 30 }),
			},
		},
		{
			name:  "empty windows",
			every: time.Minute,
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray(
					4,
					mustParseTime("2010-01-01T00:00:00Z"), 15*time.Minute,
					func(i int64) int64 { return 100 + i },
				),
			},
			want: []*cursors.IntegerArray{
				makeIntegerArray(
					4,
					mustParseTime("2010-01-01T00:01:00Z"), 15*time.Minute,
					func(i int64) int64 { return 100 + i },
				),
			},
		},
		{
			name:  "unaligned window",
			every: 15 * time.Minute,
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray(
					60,
					mustParseTime("2010-01-01T00:00:30Z"), time.Minute,
					func(i int64) int64 { return 2 },
				),
			},
			want: []*cursors.IntegerArray{
				makeIntegerArray(
					4,
					mustParseTime("2010-01-01T00:15:00Z"), 15*time.Minute,
					func(i int64) int64 {
						return 30
					}),
			},
		},
		{
			name:  "more unaligned window",
			every: 15 * time.Minute,
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray(
					60,
					mustParseTime("2010-01-01T00:01:30Z"), time.Minute,
					func(i int64) int64 { return 2 },
				),
			},
			want: []*cursors.IntegerArray{
				makeIntegerArray(
					5,
					mustParseTime("2010-01-01T00:15:00Z"), 15*time.Minute,
					func(i int64) int64 {
						switch i {
						case 0:
							return 28
						case 4:
							return 2
						default:
							return 30
						}
					}),
			},
		},
		{
			name:  "window two input arrays",
			every: 15 * time.Minute,
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray(
					60,
					mustParseTime("2010-01-01T00:00:00Z"), time.Minute,
					func(i int64) int64 { return 2 },
				),
				makeIntegerArray(
					60,
					mustParseTime("2010-01-01T01:00:00Z"), time.Minute,
					func(i int64) int64 { return 3 },
				),
			},
			want: []*cursors.IntegerArray{
				makeIntegerArray(8, mustParseTime("2010-01-01T00:15:00Z"), 15*time.Minute,
					func(i int64) int64 {
						if i < 4 {
							return 30
						} else {
							return 45
						}
					}),
			},
		},
		{
			name:  "window spans input arrays",
			every: 40 * time.Minute,
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray(
					60,
					mustParseTime("2010-01-01T00:00:00Z"), time.Minute,
					func(i int64) int64 { return 2 },
				),
				makeIntegerArray(
					60,
					mustParseTime("2010-01-01T01:00:00Z"), time.Minute,
					func(i int64) int64 { return 3 },
				),
			},
			want: []*cursors.IntegerArray{
				makeIntegerArray(3, mustParseTime("2010-01-01T00:40:00Z"), 40*time.Minute,
					func(i int64) int64 {
						switch i {
						case 0:
							return 80
						case 1:
							return 100
						case 2:
							return 120
						}
						return -1
					}),
			},
		},
		{
			name:  "more windows than MaxPointsPerBlock",
			every: 2 * time.Millisecond,
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray( // 1 second, one point per ms
					1000,
					mustParseTime("2010-01-01T00:00:00Z"), time.Millisecond,
					func(i int64) int64 { return 2 },
				),
				makeIntegerArray( // 1 second, one point per ms
					1000,
					mustParseTime("2010-01-01T00:00:01Z"), time.Millisecond,
					func(i int64) int64 { return 3 },
				),
				makeIntegerArray( // 1 second, one point per ms
					1000,
					mustParseTime("2010-01-01T00:00:02Z"), time.Millisecond,
					func(i int64) int64 { return 4 },
				),
			},
			want: []*cursors.IntegerArray{
				makeIntegerArray(
					1000,
					mustParseTime("2010-01-01T00:00:00.002Z"), 2*time.Millisecond,
					func(i int64) int64 {
						if i < 500 {
							return 4
						} else {
							return 6
						}
					},
				),
				makeIntegerArray(
					500,
					mustParseTime("2010-01-01T00:00:02.002Z"), 2*time.Millisecond,
					func(i int64) int64 { return 8 },
				),
			},
		},
		{
			name: "whole series",
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray(
					60,
					mustParseTime("2010-01-01T00:00:00Z"), time.Minute,
					func(i int64) int64 { return 2 },
				),
			},
			want: []*cursors.IntegerArray{
				makeIntegerArray(1, maxTimestamp, 40*time.Minute, func(i int64) int64 { return 120 }),
			},
		},
		{
			name:        "whole series no points",
			inputArrays: []*cursors.IntegerArray{{}},
			want:        []*cursors.IntegerArray{},
		},
		{
			name: "whole series two arrays",
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray(
					60,
					mustParseTime("2010-01-01T00:00:00Z"), time.Minute,
					func(i int64) int64 { return 2 },
				),
				makeIntegerArray(
					60,
					mustParseTime("2010-01-01T01:00:00Z"), time.Minute,
					func(i int64) int64 { return 3 },
				),
			},
			want: []*cursors.IntegerArray{
				makeIntegerArray(1, maxTimestamp, 40*time.Minute,
					func(int64) int64 {
						return 300
					}),
			},
		},
		{
			name: "whole series span epoch",
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray(
					120,
					mustParseTime("1969-12-31T23:00:00Z"), time.Minute,
					func(i int64) int64 { return 2 },
				),
			},
			want: []*cursors.IntegerArray{
				makeIntegerArray(1, maxTimestamp, 40*time.Minute, func(int64) int64 { return 240 }),
			},
		},
		{
			name: "whole series span epoch two arrays",
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray(
					60,
					mustParseTime("1969-12-31T23:00:00Z"), time.Minute,
					func(i int64) int64 { return 2 },
				),
				makeIntegerArray(
					60,
					mustParseTime("1970-01-01T00:00:00Z"), time.Minute,
					func(i int64) int64 { return 3 },
				),
			},
			want: []*cursors.IntegerArray{
				makeIntegerArray(1, maxTimestamp, 40*time.Minute, func(int64) int64 { return 300 }),
			},
		},
		{
			name: "whole series, with max int64 timestamp",
			inputArrays: []*cursors.IntegerArray{
				{
					Timestamps: []int64{math.MaxInt64},
					Values:     []int64{100},
				},
			},
			want: []*cursors.IntegerArray{
				{
					Timestamps: []int64{math.MaxInt64},
					Values:     []int64{100},
				},
			},
		},
	}
	for _, tc := range testcases {
		tc.createCursorFn = func(cur cursors.IntegerArrayCursor, every, offset int64) cursors.IntegerArrayCursor {
			return newIntegerWindowSumArrayCursor(cur, every, offset)
		}
		tc.run(t)
	}
}

func TestWindowMinArrayCursor(t *testing.T) {
	testcases := []aggArrayCursorTest{
		{
			name:  "no window",
			every: 0,
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray(
					60,
					mustParseTime("2010-01-01T00:00:00Z"), time.Minute,
					func(i int64) int64 { return 100 + i },
				),
			},
			want: []*cursors.IntegerArray{
				makeIntegerArray(1, mustParseTime("2010-01-01T00:00:00Z"), 0, func(int64) int64 { return 100 }),
			},
		},
		{
			name:  "no window min int",
			every: 0,
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray(
					60,
					mustParseTime("2010-01-01T00:00:00Z"), time.Minute,
					func(i int64) int64 {
						if i%2 == 0 {
							return math.MinInt64
						}
						return 0
					},
				),
			},
			want: []*cursors.IntegerArray{
				makeIntegerArray(1, mustParseTime("2010-01-01T00:00:00Z"), 0, func(int64) int64 { return math.MinInt64 }),
			},
		},
		{
			name:  "no window max int",
			every: 0,
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray(
					60,
					mustParseTime("2010-01-01T00:00:00Z"), time.Minute,
					func(i int64) int64 {
						if i%2 == 0 {
							return math.MaxInt64
						}
						return 0
					},
				),
			},
			want: []*cursors.IntegerArray{
				makeIntegerArray(1, mustParseTime("2010-01-01T00:01:00Z"), 0, func(int64) int64 { return 0 }),
			},
		},
		{
			name:  "window",
			every: time.Hour,
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray(
					16,
					mustParseTime("2010-01-01T00:00:00Z"), 15*time.Minute,
					func(i int64) int64 {
						base := (i / 4) * 100
						m := (i % 4) * 15
						return base + m
					},
				),
			},
			want: []*cursors.IntegerArray{
				makeIntegerArray(4, mustParseTime("2010-01-01T00:00:00Z"), time.Hour,
					func(i int64) int64 { return i * 100 }),
			},
		},
		{
			name:  "window desc values",
			every: time.Hour,
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray(
					16,
					mustParseTime("2010-01-01T00:00:00Z"), 15*time.Minute,
					func(i int64) int64 {
						base := (i / 4) * 100
						m := 60 - (i%4)*15
						return base + m
					},
				),
			},
			want: []*cursors.IntegerArray{
				makeIntegerArray(4, mustParseTime("2010-01-01T00:45:00Z"), time.Hour,
					func(i int64) int64 { return i*100 + 15 }),
			},
		},
		{
			name:  "window min int",
			every: time.Hour,
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray(
					16,
					mustParseTime("2010-01-01T00:00:00Z"), 15*time.Minute,
					func(i int64) int64 {
						return math.MinInt64
					},
				),
			},
			want: []*cursors.IntegerArray{
				makeIntegerArray(4, mustParseTime("2010-01-01T00:00:00Z"), time.Hour,
					func(i int64) int64 { return math.MinInt64 }),
			},
		},
		{
			name:  "window max int",
			every: time.Hour,
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray(
					16,
					mustParseTime("2010-01-01T00:00:00Z"), 15*time.Minute,
					func(i int64) int64 {
						return math.MaxInt64
					},
				),
			},
			want: []*cursors.IntegerArray{
				makeIntegerArray(4, mustParseTime("2010-01-01T00:00:00Z"), time.Hour,
					func(i int64) int64 { return math.MaxInt64 }),
			},
		},
		{
			name:  "empty window",
			every: 15 * time.Minute,
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray(
					2,
					mustParseTime("2010-01-01T00:05:00Z"), 30*time.Minute,
					func(i int64) int64 {
						return 100 + i
					},
				),
			},
			want: []*cursors.IntegerArray{
				makeIntegerArray(2, mustParseTime("2010-01-01T00:05:00Z"), 30*time.Minute,
					func(i int64) int64 { return 100 + i }),
			},
		},
	}
	for _, tc := range testcases {
		tc.createCursorFn = func(cur cursors.IntegerArrayCursor, every, offset int64) cursors.IntegerArrayCursor {
			return newIntegerWindowMinArrayCursor(cur, every, offset)
		}
		tc.run(t)
	}
}

func TestWindowMaxArrayCursor(t *testing.T) {
	testcases := []aggArrayCursorTest{
		{
			name:  "no window",
			every: 0,
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray(
					60,
					mustParseTime("2010-01-01T00:00:00Z"), time.Minute,
					func(i int64) int64 { return 100 + i },
				),
			},
			want: []*cursors.IntegerArray{
				makeIntegerArray(1, mustParseTime("2010-01-01T00:59:00Z"), 0, func(int64) int64 { return 159 }),
			},
		},
		{
			name:  "no window min int",
			every: 0,
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray(
					60,
					mustParseTime("2010-01-01T00:00:00Z"), time.Minute,
					func(i int64) int64 {
						if i%2 == 0 {
							return math.MinInt64
						}
						return 0
					},
				),
			},
			want: []*cursors.IntegerArray{
				makeIntegerArray(1, mustParseTime("2010-01-01T00:01:00Z"), 0, func(int64) int64 { return 0 }),
			},
		},
		{
			name:  "no window max int",
			every: 0,
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray(
					60,
					mustParseTime("2010-01-01T00:00:00Z"), time.Minute,
					func(i int64) int64 {
						if i%2 == 0 {
							return math.MaxInt64
						}
						return 0
					},
				),
			},
			want: []*cursors.IntegerArray{
				makeIntegerArray(1, mustParseTime("2010-01-01T00:00:00Z"), 0, func(int64) int64 { return math.MaxInt64 }),
			},
		},
		{
			name:  "window",
			every: time.Hour,
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray(
					16,
					mustParseTime("2010-01-01T00:00:00Z"), 15*time.Minute,
					func(i int64) int64 {
						base := (i / 4) * 100
						m := (i % 4) * 15
						return base + m
					},
				),
			},
			want: []*cursors.IntegerArray{
				makeIntegerArray(4, mustParseTime("2010-01-01T00:45:00Z"), time.Hour,
					func(i int64) int64 { return i*100 + 45 }),
			},
		},
		{
			name:  "window desc values",
			every: time.Hour,
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray(
					16,
					mustParseTime("2010-01-01T00:00:00Z"), 15*time.Minute,
					func(i int64) int64 {
						base := (i / 4) * 100
						m := 60 - (i%4)*15
						return base + m
					},
				),
			},
			want: []*cursors.IntegerArray{
				makeIntegerArray(4, mustParseTime("2010-01-01T00:00:00Z"), time.Hour,
					func(i int64) int64 { return i*100 + 60 }),
			},
		},
		{
			name:  "window min int",
			every: time.Hour,
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray(
					16,
					mustParseTime("2010-01-01T00:00:00Z"), 15*time.Minute,
					func(i int64) int64 {
						return math.MinInt64
					},
				),
			},
			want: []*cursors.IntegerArray{
				makeIntegerArray(4, mustParseTime("2010-01-01T00:00:00Z"), time.Hour,
					func(i int64) int64 { return math.MinInt64 }),
			},
		},
		{
			name:  "window max int",
			every: time.Hour,
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray(
					16,
					mustParseTime("2010-01-01T00:00:00Z"), 15*time.Minute,
					func(i int64) int64 {
						return math.MaxInt64
					},
				),
			},
			want: []*cursors.IntegerArray{
				makeIntegerArray(4, mustParseTime("2010-01-01T00:00:00Z"), time.Hour,
					func(i int64) int64 { return math.MaxInt64 }),
			},
		},
		{
			name:  "empty window",
			every: 15 * time.Minute,
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray(
					2,
					mustParseTime("2010-01-01T00:05:00Z"), 30*time.Minute,
					func(i int64) int64 {
						return 100 + i
					},
				),
			},
			want: []*cursors.IntegerArray{
				makeIntegerArray(2, mustParseTime("2010-01-01T00:05:00Z"), 30*time.Minute,
					func(i int64) int64 { return 100 + i }),
			},
		},
	}
	for _, tc := range testcases {
		tc.createCursorFn = func(cur cursors.IntegerArrayCursor, every, offset int64) cursors.IntegerArrayCursor {
			return newIntegerWindowMaxArrayCursor(cur, every, offset)
		}
		tc.run(t)
	}
}

type MockExpression struct {
	EvalBoolFunc func(v Valuer) bool
}

func (e *MockExpression) EvalBool(v Valuer) bool { return e.EvalBoolFunc(v) }
