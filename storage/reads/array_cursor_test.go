package reads

import (
	"math"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/v2/storage/reads/datatypes"
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

func TestIntegerIntegerCountArrayCursor(t *testing.T) {
	maxTimestamp := time.Unix(0, math.MaxInt64)

	testcases := []struct {
		name        string
		every       time.Duration
		inputArrays []*cursors.IntegerArray
		want        []*cursors.IntegerArray
	}{
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
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			var resultN int
			mc := &MockIntegerArrayCursor{
				CloseFunc: func() {},
				ErrFunc:   func() error { return nil },
				StatsFunc: func() cursors.CursorStats { return cursors.CursorStats{} },
				NextFunc: func() *cursors.IntegerArray {
					if resultN < len(tc.inputArrays) {
						a := tc.inputArrays[resultN]
						resultN++
						return a
					}
					return &cursors.IntegerArray{}
				},
			}
			countArrayCursor := newIntegerIntegerWindowCountArrayCursor(mc, int64(tc.every))
			got := make([]*cursors.IntegerArray, 0, len(tc.want))
			for a := countArrayCursor.Next(); a.Len() != 0; a = countArrayCursor.Next() {
				got = append(got, copyIntegerArray(a))
			}

			if diff := cmp.Diff(got, tc.want); diff != "" {
				t.Fatalf("did not get expected result from count array cursor; -got/+want:\n%v", diff)
			}
		})
	}
}

func TestNewCountArrayCursor(t *testing.T) {
	want := &integerIntegerWindowCountArrayCursor{
		IntegerArrayCursor: &MockIntegerArrayCursor{},
		res:                cursors.NewIntegerArrayLen(1),
		tmp:                &cursors.IntegerArray{},
	}

	got := newCountArrayCursor(&MockIntegerArrayCursor{})

	if diff := cmp.Diff(got, want, cmp.AllowUnexported(integerIntegerWindowCountArrayCursor{})); diff != "" {
		t.Fatalf("did not get expected cursor; -got/+want:\n%v", diff)
	}
}

func TestNewWindowCountArrayCursor(t *testing.T) {
	t.Run("hour window", func(t *testing.T) {
		want := &integerIntegerWindowCountArrayCursor{
			IntegerArrayCursor: &MockIntegerArrayCursor{},
			every:              int64(time.Hour),
			res:                cursors.NewIntegerArrayLen(MaxPointsPerBlock),
			tmp:                &cursors.IntegerArray{},
		}

		req := &datatypes.ReadWindowAggregateRequest{
			WindowEvery: int64(time.Hour),
		}
		got := newWindowCountArrayCursor(&MockIntegerArrayCursor{}, req)

		if diff := cmp.Diff(got, want, cmp.AllowUnexported(integerIntegerWindowCountArrayCursor{})); diff != "" {
			t.Fatalf("did not get expected cursor; -got/+want:\n%v", diff)
		}
	})

	t.Run("count whole series", func(t *testing.T) {
		want := &integerIntegerWindowCountArrayCursor{
			IntegerArrayCursor: &MockIntegerArrayCursor{},
			every:              0,
			res:                cursors.NewIntegerArrayLen(1),
			tmp:                &cursors.IntegerArray{},
		}

		req := &datatypes.ReadWindowAggregateRequest{
			WindowEvery: math.MaxInt64,
		}
		got := newWindowCountArrayCursor(&MockIntegerArrayCursor{}, req)

		if diff := cmp.Diff(got, want, cmp.AllowUnexported(integerIntegerWindowCountArrayCursor{})); diff != "" {
			t.Fatalf("did not get expected cursor; -got/+want:\n%v", diff)
		}
	})
}

type MockIntegerArrayCursor struct {
	CloseFunc func()
	ErrFunc   func() error
	StatsFunc func() cursors.CursorStats
	NextFunc  func() *cursors.IntegerArray
}

func (c *MockIntegerArrayCursor) Close()                      { c.CloseFunc() }
func (c *MockIntegerArrayCursor) Err() error                  { return c.ErrFunc() }
func (c *MockIntegerArrayCursor) Stats() cursors.CursorStats  { return c.StatsFunc() }
func (c *MockIntegerArrayCursor) Next() *cursors.IntegerArray { return c.NextFunc() }

type MockExpression struct {
	EvalBoolFunc func(v Valuer) bool
}

func (e *MockExpression) EvalBool(v Valuer) bool { return e.EvalBoolFunc(v) }
