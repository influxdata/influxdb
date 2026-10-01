package query

import (
	"math"
	"testing"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxql"
	"github.com/stretchr/testify/require"
)

func TestDatePartMap_Value(t *testing.T) {
	// 2023-01-16T10:30:45Z — a Monday.
	ts := time.Date(2023, 1, 16, 10, 30, 45, 0, time.UTC).UnixNano()
	row := &Row{Time: ts}

	require.Equal(t, int64(2023), datePartMap{expr: Year, loc: time.UTC}.Value(row))
	require.Equal(t, int64(1), datePartMap{expr: Month, loc: time.UTC}.Value(row))
	require.Equal(t, int64(10), datePartMap{expr: Hour, loc: time.UTC}.Value(row))
	require.Equal(t, int64(1), datePartMap{expr: DOW, loc: time.UTC}.Value(row)) // Monday = 1

	// nil location is treated as UTC.
	require.Equal(t, int64(2023), datePartMap{expr: Year, loc: nil}.Value(row))

	// Non-UTC location shifts the hour. America/New_York is UTC-5 in January.
	ny, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	require.Equal(t, int64(5), datePartMap{expr: Hour, loc: ny}.Value(row)) // 10:30 UTC -> 05:30 EST

	// An unknown expr (e.g. deserialized from a newer peer's iterator options)
	// must yield nil so the grouper rejects it loudly instead of silently
	// grouping every row under value 0.
	require.Nil(t, datePartMap{expr: Invalid, loc: time.UTC}.Value(row))
}

func TestNewDatePartCondition(t *testing.T) {
	t.Run("nil condition", func(t *testing.T) {
		require.Nil(t, NewDatePartCondition(nil, nil))
	})

	t.Run("condition without date_part", func(t *testing.T) {
		require.Nil(t, NewDatePartCondition(influxql.MustParseExpr(`f1 > 0`), nil))
	})

	t.Run("rewrite removes calls and preserves the original", func(t *testing.T) {
		orig := influxql.MustParseExpr(`f1 > 0 AND date_part('hour', time) < 12`)
		origStr := orig.String()

		c := NewDatePartCondition(orig, nil)
		require.NotNil(t, c)
		require.Equal(t, origStr, orig.String())

		influxql.WalkFunc(c.Expr(), func(n influxql.Node) {
			_, ok := n.(*influxql.Call)
			require.False(t, ok, "rewritten condition must not contain calls")
		})
	})

	t.Run("dedupes repeated parts", func(t *testing.T) {
		c := NewDatePartCondition(influxql.MustParseExpr(
			`date_part('hour', time) >= 9 AND date_part('hour', time) < 17`), nil)
		require.NotNil(t, c)
		require.Len(t, c.parts, 1)
	})
}

func TestDatePartCondition_MatchesDatePartValuer(t *testing.T) {
	la, err := time.LoadLocation("America/Los_Angeles")
	require.NoError(t, err)

	conds := []string{
		`date_part('hour', time) < 12`,
		`date_part('hour', time) >= 9 AND date_part('hour', time) < 17`,
		`date_part('dow', time) = 0 OR date_part('dow', time) = 6`,
		`date_part('hour', time) + 1 < 13`,
		`date_part('month', time) = 1 AND f1 > 0`,
		`date_part('year', time) = 2023`,
		`date_part('dow', time) = 0 AND date_part('hour', time) < 12`,
	}
	timestamps := []time.Time{
		time.Date(2023, 1, 1, 3, 0, 0, 0, time.UTC),   // Sunday 03:00
		time.Date(2023, 1, 2, 15, 30, 0, 0, time.UTC), // Monday 15:30
		time.Date(2024, 6, 7, 23, 59, 59, 0, time.UTC),
	}

	for _, condStr := range conds {
		for _, loc := range []*time.Location{nil, la} {
			for _, ts := range timestamps {
				cond := influxql.MustParseExpr(condStr)

				legacyM := map[string]interface{}{
					"f1":              int64(5),
					models.TimeString: ts.UnixNano(),
				}
				legacy := influxql.ValuerEval{
					Valuer: influxql.MultiValuer(
						MathValuer{},
						DatePartValuer{Location: loc},
						influxql.MapValuer(legacyM),
					),
				}
				want := (&legacy).EvalBool(cond)

				c := NewDatePartCondition(cond, loc)
				require.NotNil(t, c, condStr)
				m := map[string]interface{}{"f1": int64(5)}
				c.SetTime(ts.UnixNano(), m)
				newEval := influxql.ValuerEval{
					Valuer: influxql.MultiValuer(
						MathValuer{},
						influxql.MapValuer(m),
					),
				}
				got := (&newEval).EvalBool(c.Expr())

				require.Equal(t, want, got, "%s at %s in %v", condStr, ts, loc)
			}
		}
	}
}

// The year (2026) stays constant and is above the runtime's small-integer
// boxing cache, so publishing it without the part's own cache would allocate.
func TestDatePartCondition_SetTime_ZeroAllocs(t *testing.T) {
	c := NewDatePartCondition(influxql.MustParseExpr(`date_part('year', time) > 0`), nil)
	require.NotNil(t, c)

	m := make(map[string]interface{})
	ts := time.Date(2026, 7, 6, 3, 0, 0, 0, time.UTC).UnixNano()
	c.SetTime(ts, m) // prime the boxing cache

	allocs := testing.AllocsPerRun(1000, func() {
		ts += int64(time.Second)
		c.SetTime(ts, m)
	})
	require.Zero(t, allocs)
}

func TestFilterCursor_DatePartCondition(t *testing.T) {
	cols := []influxql.VarRef{{Val: "value", Type: influxql.Float}}
	rows := []Row{
		{Time: time.Date(2023, 1, 1, 3, 0, 0, 0, time.UTC).UnixNano(), Values: []interface{}{1.0}},
		{Time: time.Date(2023, 1, 1, 15, 0, 0, 0, time.UTC).UnixNano(), Values: []interface{}{2.0}},
		{Time: time.Date(2023, 1, 2, 5, 0, 0, 0, time.UTC).UnixNano(), Values: []interface{}{3.0}},
	}

	t.Run("date_part filter", func(t *testing.T) {
		cur := newFilterCursor(
			RowCursor(rows, cols),
			influxql.MustParseExpr(`date_part('hour', time) < 12`),
			true,
			nil,
		)
		var row Row
		var got []interface{}
		for cur.Scan(&row) {
			got = append(got, row.Values[0])
		}
		require.Equal(t, []interface{}{1.0, 3.0}, got)
	})

	t.Run("date_part filter with location", func(t *testing.T) {
		la, err := time.LoadLocation("America/Los_Angeles")
		require.NoError(t, err)
		// 03:00 UTC = 19:00 previous day PST; 15:00 UTC = 07:00 PST.
		cur := newFilterCursor(
			RowCursor(rows, cols),
			influxql.MustParseExpr(`date_part('hour', time) < 12`),
			true,
			la,
		)
		var row Row
		var got []interface{}
		for cur.Scan(&row) {
			got = append(got, row.Values[0])
		}
		require.Equal(t, []interface{}{2.0}, got)
	})

	t.Run("non-date_part filter unchanged", func(t *testing.T) {
		cur := newFilterCursor(
			RowCursor(rows, cols),
			influxql.MustParseExpr(`value > 1`),
			false,
			nil,
		)
		require.Nil(t, cur.dpCond)
		var row Row
		var got []interface{}
		for cur.Scan(&row) {
			got = append(got, row.Values[0])
		}
		require.Equal(t, []interface{}{2.0, 3.0}, got)
	})
}

func TestFilterCursor_DatePart_ZeroAllocs(t *testing.T) {
	cols := []influxql.VarRef{{Val: "value", Type: influxql.Float}}
	base := time.Date(2026, 7, 6, 3, 0, 0, 0, time.UTC).UnixNano()
	rows := make([]Row, 2048)
	for i := range rows {
		rows[i] = Row{Time: base + int64(i)*int64(time.Second), Values: []interface{}{1.0}}
	}
	cur := newFilterCursor(RowCursor(rows, cols), influxql.MustParseExpr(`date_part('hour', time) < 12`), true, nil)

	var row Row
	require.True(t, cur.Scan(&row)) // prime the boxing cache

	allocs := testing.AllocsPerRun(1000, func() {
		cur.Scan(&row)
	})
	require.Zero(t, allocs)
}

// TestComputeDimKey_SignedValueOrdering ensures DimKeys sort lexicographically
// in the same order as their signed values. The reduce path sorts DimKey
// strings to order the emitted series, so a negative value (e.g. a pre-1970
// 'epoch') must produce a key that sorts before every non-negative value's key.
func TestComputeDimKey_SignedValueOrdering(t *testing.T) {
	vals := []int64{math.MinInt64, -100, -1, 0, 1, 100, math.MaxInt64}
	for _, hasTags := range []bool{false, true} {
		var prev string
		for i, v := range vals {
			key := computeDimKey(Epoch, v, TagSubset{ID: "tagid", HasTags: hasTags})
			if i > 0 {
				require.Less(t, prev, key,
					"DimKey for %d must sort before DimKey for %d (hasTags=%v)", vals[i-1], v, hasTags)
			}
			prev = key
		}
	}
}

// BenchmarkFilterCursor_DatePartCondition measures the per-row cost of a
// date_part filter at the subquery boundary. Timestamps advance one second per
// row so boxed values are realistic.
func BenchmarkFilterCursor_DatePartCondition(b *testing.B) {
	cols := []influxql.VarRef{{Val: "value", Type: influxql.Float}}
	base := time.Date(2026, 7, 6, 0, 0, 0, 0, time.UTC).UnixNano()
	rows := make([]Row, b.N)
	for i := range rows {
		rows[i] = Row{Time: base + int64(i)*int64(time.Second), Values: []interface{}{1.0}}
	}
	cur := newFilterCursor(RowCursor(rows, cols), influxql.MustParseExpr(`date_part('hour', time) < 12`), true, nil)

	b.ResetTimer()
	b.ReportAllocs()
	var row Row
	for cur.Scan(&row) {
	}
}

// --- Direct unit tests for the date_part block in *Reduce*Iterator.reduce ---
//
// The end-to-end server tests exercise the happy path via response JSON, but the
// ResolveKeys error return and the Aux-width defensive guards are never taken
// there. These tests drive a float reduce iterator directly with a stub
// reducer, asserting on the emitted Aux slots.

// sliceFloatIterator is a minimal FloatIterator that replays a fixed slice.
type sliceFloatIterator struct {
	points []FloatPoint
	i      int
}

func (it *sliceFloatIterator) Next() (*FloatPoint, error) {
	if it.i >= len(it.points) {
		return nil, nil
	}
	p := it.points[it.i]
	it.i++
	return &p, nil
}
func (it *sliceFloatIterator) Stats() IteratorStats { return IteratorStats{} }
func (it *sliceFloatIterator) Close() error         { return nil }

// stubReducer implements both FloatPointAggregator and FloatPointEmitter. Emit
// returns a single point whose Aux is a fresh copy of the configured template,
// letting each test control the width the reduce guards must normalize.
type stubReducer struct {
	emitAux []interface{}
}

func (r *stubReducer) AggregateFloat(p *FloatPoint) {}
func (r *stubReducer) Emit() []FloatPoint {
	return []FloatPoint{{Aux: append([]interface{}(nil), r.emitAux...)}}
}

// drainReduceIterator runs one input point per inputAux entry through a float
// reduce iterator and returns the emitted points.
func drainReduceIterator(t *testing.T, opt IteratorOptions, inputAux [][]interface{}, reducerAux []interface{}) ([]FloatPoint, error) {
	t.Helper()
	input := &sliceFloatIterator{}
	for _, aux := range inputAux {
		input.points = append(input.points, FloatPoint{Name: "cpu", Time: 0, Aux: aux})
	}
	create := func() (FloatPointAggregator, FloatPointEmitter) {
		r := &stubReducer{emitAux: reducerAux}
		return r, r
	}
	itr := newFloatReduceFloatIterator(input, opt, create)
	var got []FloatPoint
	for {
		p, err := itr.Next()
		if err != nil {
			return got, err
		}
		if p == nil {
			return got, nil
		}
		got = append(got, *p)
	}
}

func datePartReduceOptions(ascending bool, auxLen int, dims ...DatePartExpr) IteratorOptions {
	var dpDims []DatePartDimension
	for _, d := range dims {
		dpDims = append(dpDims, DatePartDimension{Expr: d})
	}
	return IteratorOptions{
		StartTime: 0,
		EndTime:   1 << 62,
		Ascending: ascending,
		Ordered:   true,
		Aux:       make([]influxql.VarRef, auxLen),
		DatePart:  NewDatePartGrouper(dpDims),
	}
}

func TestReduceIterator_DatePart_Aux(t *testing.T) {
	tests := []struct {
		name     string
		auxLen   int             // len(opt.Aux) — the scanner key count
		dims     []DatePartExpr  // GROUP BY date_part dimensions
		inputAux []interface{}   // the input point's Aux; date_part values last
		emitAux  []interface{}   // Aux the reducer's Emit returns
		want     [][]interface{} // expected Aux of each emitted point
	}{
		{
			// Aggregate (COUNT/SUM) emits an empty Aux: it must grow to the full
			// scanner-key width with the active value in its dimension's slot.
			name:   "aggregate widens empty aux to full width",
			auxLen: 3, dims: []DatePartExpr{Month}, inputAux: []interface{}{int64(3)}, emitAux: nil,
			want: [][]interface{}{{nil, nil, int64(3)}},
		},
		{
			// Selector (MIN/MAX) emits a full-width Aux: the leading field slots
			// are preserved and only the date_part slots are overwritten.
			name:   "selector full-width aux preserves leading slots",
			auxLen: 3, dims: []DatePartExpr{Month}, inputAux: []interface{}{int64(3)}, emitAux: []interface{}{"a", "b", "c"},
			want: [][]interface{}{{"a", "b", int64(3)}},
		},
		{
			// With multiple date_part dimensions each series carries its value in
			// its own dimension's slot and nulls the others, so a value can't
			// leak into a non-active column.
			name:   "multi-dimension fills only the active slot",
			auxLen: 3, dims: []DatePartExpr{Year, Month}, inputAux: []interface{}{int64(2026), int64(3)}, emitAux: []interface{}{"a", "b", "c"},
			want: [][]interface{}{{"a", nil, int64(3)}, {"a", int64(2026), nil}},
		},
		{
			// Fewer scanner keys than dimensions: the Aux still grows to hold a
			// slot for every dimension rather than indexing out of range.
			name:   "aux widens to hold every dimension",
			auxLen: 1, dims: []DatePartExpr{Year, Month}, inputAux: []interface{}{int64(2026), int64(3)}, emitAux: nil,
			want: [][]interface{}{{nil, int64(3)}, {int64(2026), nil}},
		},
		{
			// An emitted Aux longer than the scanner key set keeps the longer width.
			name:   "longer emitted aux keeps its width",
			auxLen: 2, dims: []DatePartExpr{Month}, inputAux: []interface{}{int64(3)}, emitAux: []interface{}{"a", "b", "c", "d"},
			want: [][]interface{}{{"a", "b", "c", int64(3)}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			opt := datePartReduceOptions(true, tc.auxLen, tc.dims...)
			got, err := drainReduceIterator(t, opt, [][]interface{}{tc.inputAux}, tc.emitAux)
			require.NoError(t, err)
			require.Len(t, got, len(tc.want))
			for i := range tc.want {
				require.Equal(t, tc.want[i], got[i].Aux)
			}
		})
	}
}

func TestReduceIterator_DatePart_ResolveKeysError(t *testing.T) {
	opt := datePartReduceOptions(true, 1, Month)
	_, err := drainReduceIterator(t, opt, [][]interface{}{{"not an int"}}, nil)
	require.ErrorContains(t, err, "unexpected aux value type")
}

// A reduce above another (e.g. over the merged shard outputs) receives points
// that carry only their bucket's dimension value, the other dimension slots
// nil. Each must regroup under that one dimension, not fail or drop the point.
func TestReduceIterator_DatePart_Regroup(t *testing.T) {
	opt := datePartReduceOptions(true, 2, Year, Month)
	got, err := drainReduceIterator(t, opt, [][]interface{}{{nil, int64(3)}, {int64(2026), nil}}, nil)
	require.NoError(t, err)
	require.Len(t, got, 2)
	require.Equal(t, []interface{}{nil, int64(3)}, got[0].Aux)
	require.Equal(t, []interface{}{int64(2026), nil}, got[1].Aux)
}

// TestReduceIterator_DatePart_SortOrder covers the key sort in reduce whose
// sort.Reverse is conditional on opt.Ascending. The emitted series order is
// governed by the string sort of the bucket keys, so it flips with the scan
// direction. The server-level DST_Descending test exercises the same branch
// end-to-end; this asserts it directly on the emitted Aux.
func TestReduceIterator_DatePart_SortOrder(t *testing.T) {
	low := int64(1)
	high := int64(3)

	// The active date_part value lands in the last Aux slot, so the ordered
	// tail across the two emitted points reveals the series order.
	tail := func(pts []FloatPoint) []interface{} {
		out := make([]interface{}, len(pts))
		for i, p := range pts {
			out[i] = p.Aux[len(p.Aux)-1]
		}
		return out
	}

	tests := []struct {
		name      string
		ascending bool
		want      []interface{}
	}{
		{
			// Ascending scan: the lower hour emits first (ascending series order).
			name: "ascending emits low hour first", ascending: true,
			want: []interface{}{low, high},
		},
		{
			// Descending scan (ORDER BY time DESC): the Ascending-conditional
			// sort.Reverse is skipped, flipping the series order — high hour first.
			name: "descending emits high hour first", ascending: false,
			want: []interface{}{high, low},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			opt := datePartReduceOptions(tc.ascending, 1, Hour)
			got, err := drainReduceIterator(t, opt, [][]interface{}{{int64(1)}, {int64(3)}}, nil)
			require.NoError(t, err)
			require.Len(t, got, 2)
			require.Equal(t, tc.want, tail(got))
		})
	}
}
