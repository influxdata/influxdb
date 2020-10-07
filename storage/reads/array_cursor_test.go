package reads

import (
	"math"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/values"
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

func makeFloatArray(n int, tsStart time.Time, tsStep time.Duration, valueFn func(i int64) float64) *cursors.FloatArray {
	fa := &cursors.FloatArray{
		Timestamps: make([]int64, n),
		Values:     make([]float64, n),
	}

	for i := 0; i < n; i++ {
		fa.Timestamps[i] = tsStart.UnixNano() + int64(i)*int64(tsStep)
		fa.Values[i] = valueFn(int64(i))
	}

	return fa
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

func copyFloatArray(src *cursors.FloatArray) *cursors.FloatArray {
	dst := cursors.NewFloatArrayLen(src.Len())
	copy(dst.Timestamps, src.Timestamps)
	copy(dst.Values, src.Values)
	return dst
}

type aggArrayCursorTest struct {
	name           string
	createCursorFn func(cur cursors.IntegerArrayCursor, every, offset int64, window execute.Window) cursors.Cursor
	every          time.Duration
	offset         time.Duration
	inputArrays    []*cursors.IntegerArray
	wantIntegers   []*cursors.IntegerArray
	wantFloats     []*cursors.FloatArray
	window         execute.Window
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
		c := a.createCursorFn(mc, int64(a.every), int64(a.offset), a.window)
		switch cursor := c.(type) {
		case cursors.IntegerArrayCursor:
			got := make([]*cursors.IntegerArray, 0, len(a.wantIntegers))
			for a := cursor.Next(); a.Len() != 0; a = cursor.Next() {
				got = append(got, copyIntegerArray(a))
			}

			if diff := cmp.Diff(got, a.wantIntegers); diff != "" {
				t.Fatalf("did not get expected result from count array cursor; -got/+want:\n%v", diff)
			}
		case cursors.FloatArrayCursor:
			got := make([]*cursors.FloatArray, 0, len(a.wantFloats))
			for a := cursor.Next(); a.Len() != 0; a = cursor.Next() {
				got = append(got, copyFloatArray(a))
			}

			if diff := cmp.Diff(got, a.wantFloats); diff != "" {
				t.Fatalf("did not get expected result from count array cursor; -got/+want:\n%v", diff)
			}
		default:
			t.Fatalf("unsupported cursor type: %T", cursor)
		}
	})
}

func TestWindowMeanArrayCursor(t *testing.T) {
	maxTimestamp := time.Unix(0, math.MaxInt64)

	testcases := []aggArrayCursorTest{
		{
			name:  "no window",
			every: 0,
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray(
					5,
					mustParseTime("2010-01-01T00:00:00Z"), time.Minute,
					func(i int64) int64 { return i + 1 },
				),
			},
			wantFloats: []*cursors.FloatArray{
				makeFloatArray(1, maxTimestamp, 0, func(int64) float64 { return 3.0 }),
			},
		},
		{
			name:  "no window fraction result",
			every: 0,
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray(
					6,
					mustParseTime("2010-01-01T00:00:00Z"), time.Minute,
					func(i int64) int64 { return i + 1 },
				),
			},
			wantFloats: []*cursors.FloatArray{
				makeFloatArray(1, maxTimestamp, 0, func(int64) float64 { return 3.5 }),
			},
		},
		{
			name:        "no window empty",
			every:       0,
			inputArrays: []*cursors.IntegerArray{},
			wantFloats:  []*cursors.FloatArray{},
		},
		{
			name:  "window",
			every: 30 * time.Minute,
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray(
					8,
					mustParseTime("2010-01-01T00:00:00Z"), 15*time.Minute,
					func(i int64) int64 {
						return i
					},
				),
			},
			wantFloats: []*cursors.FloatArray{
				makeFloatArray(4, mustParseTime("2010-01-01T00:30:00Z"), 30*time.Minute,
					func(i int64) float64 { return 0.5 + float64(i)*2 }),
			},
		},
		{
			name:   "window offset",
			every:  30 * time.Minute,
			offset: 5 * time.Minute,
			inputArrays: []*cursors.IntegerArray{
				makeIntegerArray(
					8,
					mustParseTime("2010-01-01T00:00:00Z"), 15*time.Minute,
					func(i int64) int64 {
						return i
					},
				),
			},
			wantFloats: []*cursors.FloatArray{
				makeFloatArray(5, mustParseTime("2010-01-01T00:05:00Z"), 30*time.Minute,
					func(i int64) float64 { return []float64{0, 1.5, 3.5, 5.5, 7}[i] }),
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
			wantFloats: []*cursors.FloatArray{
				makeFloatArray(2, mustParseTime("2010-01-01T00:15:00Z"), 30*time.Minute,
					func(i int64) float64 { return 100 + float64(i) }),
			},
		},
	}
	for _, tc := range testcases {
		tc.createCursorFn = func(cur cursors.IntegerArrayCursor, every, offset int64, window execute.Window) cursors.Cursor {
			if every != 0 || offset != 0 {
				everyDur := values.MakeDuration(every, 0, false)
				offsetDur := values.MakeDuration(offset, 0, false)
				window = execute.Window{
					Every:  everyDur,
					Offset: offsetDur,
				}
			}
			return newIntegerWindowMeanArrayCursor(cur, window)
		}
		tc.run(t)
	}
}

type MockExpression struct {
	EvalBoolFunc func(v Valuer) bool
}

func (e *MockExpression) EvalBool(v Valuer) bool { return e.EvalBoolFunc(v) }
