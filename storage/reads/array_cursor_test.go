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

func makeIntegerArray(n int64, tsStart time.Time, tsStep time.Duration, vStart, vStep int64) *cursors.IntegerArray {
	ia := &cursors.IntegerArray{
		Timestamps: make([]int64, n),
		Values:     make([]int64, n),
	}

	for i := int64(0); i < n; i++ {
		ia.Timestamps[i] = tsStart.UnixNano() + i*int64(tsStep)
		ia.Values[i] = vStart + i*vStep
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
					100, 1,
				),
			},
			want: []*cursors.IntegerArray{
				makeIntegerArray(4, mustParseTime("2010-01-01T00:15:00Z"), 15*time.Minute, 15, 0),
			},
		},
		{
			name:  "window two input arrays",
			every: 15 * time.Minute,
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray(
					60,
					mustParseTime("2010-01-01T00:00:00Z"), time.Minute,
					100, 1,
				),
				makeIntegerArray(
					60,
					mustParseTime("2010-01-01T01:00:00Z"), time.Minute,
					200, 1,
				),
			},
			want: []*cursors.IntegerArray{
				makeIntegerArray(8, mustParseTime("2010-01-01T00:15:00Z"), 15*time.Minute, 15, 0),
			},
		},
		{
			name:  "window spans input arrays",
			every: 40 * time.Minute,
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray(
					60,
					mustParseTime("2010-01-01T00:00:00Z"), time.Minute,
					100, 1,
				),
				makeIntegerArray(
					60,
					mustParseTime("2010-01-01T01:00:00Z"), time.Minute,
					200, 1,
				),
			},
			want: []*cursors.IntegerArray{
				makeIntegerArray(3, mustParseTime("2010-01-01T00:40:00Z"), 40*time.Minute, 40, 0),
			},
		},
		{
			name:  "window max int every",
			every: time.Duration(math.MaxInt64),
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray(
					60,
					mustParseTime("2010-01-01T00:00:00Z"), time.Minute,
					100, 1,
				),
			},
			want: []*cursors.IntegerArray{
				makeIntegerArray(1, maxTimestamp, 40*time.Minute, 60, 0),
			},
		},
		{
			name:  "window max int every two arrays",
			every: time.Duration(math.MaxInt64),
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray(
					60,
					mustParseTime("2010-01-01T00:00:00Z"), time.Minute,
					100, 1,
				),
				makeIntegerArray(
					60,
					mustParseTime("2010-01-01T01:00:00Z"), time.Minute,
					100, 1,
				),
			},
			want: []*cursors.IntegerArray{
				makeIntegerArray(1, maxTimestamp, 40*time.Minute, 120, 0),
			},
		},
		{
			name:  "window max int span epoch",
			every: time.Duration(math.MaxInt64),
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray(
					120,
					mustParseTime("1969-12-31T23:00:00Z"), time.Minute,
					100, 1,
				),
			},
			want: []*cursors.IntegerArray{
				makeIntegerArray(1, maxTimestamp, 40*time.Minute, 120, 0),
			},
		},
		{
			name:  "window max int span epoch two arrays",
			every: time.Duration(math.MaxInt64),
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray(
					60,
					mustParseTime("1969-12-31T23:00:00Z"), time.Minute,
					100, 1,
				),
				makeIntegerArray(
					60,
					mustParseTime("1970-01-01T00:00:00Z"), time.Minute,
					100, 1,
				),
			},
			want: []*cursors.IntegerArray{
				makeIntegerArray(1, maxTimestamp, 40*time.Minute, 120, 0),
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
			var countArrayCursor cursors.IntegerArrayCursor
			if tc.every != 0 {
				countArrayCursor = &integerIntegerWindowCountArrayCursor{
					IntegerArrayCursor: mc,
					every:              int64(tc.every),
				}
			} else {
				countArrayCursor = newCountArrayCursor(mc).(cursors.IntegerArrayCursor)
			}
			got := make([]*cursors.IntegerArray, 0, len(tc.want))
			for a := countArrayCursor.Next(); a.Len() != 0; a = countArrayCursor.Next() {
				got = append(got, a)
			}

			if diff := cmp.Diff(got, tc.want); diff != "" {
				t.Fatalf("did not get expected result from count array cursor; -got/+want:\n%v", diff)
			}
		})
	}
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
