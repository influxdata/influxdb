package query

import (
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
}

// TestEncodeDecodeAux_DatePartKey ensures a DecodedDatePartKey grouping value
// survives the iterator wire codec (encodeAux/decodeAux). This is the codec used
// to stream iterators between enterprise data nodes; without explicit handling the
// key serializes to null and all date_part GROUP BY buckets collapse into one.
func TestEncodeDecodeAux_DatePartKey(t *testing.T) {
	// Use a non-zero Expr (Month) so the expr byte is actually exercised, alongside
	// neighbouring aux values of other types.
	key := DecodedDatePartKey{Expr: Month, Val: 12}
	aux := []interface{}{int64(7), key, "host1"}

	got := decodeAux(encodeAux(aux))

	require.Len(t, got, 3)
	require.Equal(t, int64(7), got[0])
	require.Equal(t, key, got[1], "DecodedDatePartKey must survive the iterator wire codec")
	require.Equal(t, "host1", got[2])
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

func TestDatePartCondition_SetTime_ZeroAllocs(t *testing.T) {
	c := NewDatePartCondition(influxql.MustParseExpr(`date_part('hour', time) < 12`), nil)
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
	cur := newFilterCursor(RowCursor(rows, cols), influxql.MustParseExpr(`date_part('hour', time) < 12`), nil)

	var row Row
	require.True(t, cur.Scan(&row)) // prime the boxing cache

	allocs := testing.AllocsPerRun(1000, func() {
		cur.Scan(&row)
	})
	require.Zero(t, allocs)
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
	cur := newFilterCursor(RowCursor(rows, cols), influxql.MustParseExpr(`date_part('hour', time) < 12`), nil)

	b.ResetTimer()
	b.ReportAllocs()
	var row Row
	for cur.Scan(&row) {
	}
}
