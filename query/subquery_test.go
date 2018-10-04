package query_test

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxql"
)

type CreateIteratorFn func(ctx context.Context, m *influxql.Measurement, opt query.IteratorOptions) query.Iterator

func TestSubquery(t *testing.T) {
	for _, test := range []struct {
		Name        string
		Statement   string
		Fields      map[string]influxql.DataType
		MapShardsFn func(t *testing.T, tr influxql.TimeRange) CreateIteratorFn
		Rows        []query.Row
	}{
		{
			Name:      "AuxiliaryFields",
			Statement: `SELECT max / 2.0 FROM (SELECT max(value) FROM cpu GROUP BY time(5s)) WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:15Z'`,
			Fields:    map[string]influxql.DataType{"value": influxql.Float},
			MapShardsFn: func(t *testing.T, tr influxql.TimeRange) CreateIteratorFn {
				if got, want := tr.MinTimeNano(), 0*Second; got != want {
					t.Errorf("unexpected min time: got=%d want=%d", got, want)
				}
				if got, want := tr.MaxTimeNano(), 15*Second-1; got != want {
					t.Errorf("unexpected max time: got=%d want=%d", got, want)
				}
				return func(ctx context.Context, m *influxql.Measurement, opt query.IteratorOptions) query.Iterator {
					if got, want := m.Name, "cpu"; got != want {
						t.Errorf("unexpected source: got=%s want=%s", got, want)
					}
					if got, want := opt.Expr.String(), "max(value::float)"; got != want {
						t.Errorf("unexpected expression: got=%s want=%s", got, want)
					}
					return &FloatIterator{Points: []query.FloatPoint{
						{Name: "cpu", Time: 0 * Second, Value: 5},
						{Name: "cpu", Time: 5 * Second, Value: 3},
						{Name: "cpu", Time: 10 * Second, Value: 8},
					}}
				}
			},
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "cpu"}, Values: []interface{}{2.5}},
				{Time: 5 * Second, Series: query.Series{Name: "cpu"}, Values: []interface{}{1.5}},
				{Time: 10 * Second, Series: query.Series{Name: "cpu"}, Values: []interface{}{float64(4)}},
			},
		},
		{
			Name:      "AuxiliaryFields_WithWhereClause",
			Statement: `SELECT host FROM (SELECT max(value), host FROM cpu GROUP BY time(5s)) WHERE max > 4 AND time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:15Z'`,
			Fields: map[string]influxql.DataType{
				"value": influxql.Float,
				"host":  influxql.Tag,
			},
			MapShardsFn: func(t *testing.T, tr influxql.TimeRange) CreateIteratorFn {
				if got, want := tr.MinTimeNano(), 0*Second; got != want {
					t.Errorf("unexpected min time: got=%d want=%d", got, want)
				}
				if got, want := tr.MaxTimeNano(), 15*Second-1; got != want {
					t.Errorf("unexpected max time: got=%d want=%d", got, want)
				}
				return func(ctx context.Context, m *influxql.Measurement, opt query.IteratorOptions) query.Iterator {
					if got, want := m.Name, "cpu"; got != want {
						t.Errorf("unexpected source: got=%s want=%s", got, want)
					}
					if got, want := opt.Expr.String(), "max(value::float)"; got != want {
						t.Errorf("unexpected expression: got=%s want=%s", got, want)
					}
					if got, want := opt.Aux, []influxql.VarRef{{Val: "host", Type: influxql.Tag}}; !cmp.Equal(got, want) {
						t.Errorf("unexpected auxiliary fields:\n%s", cmp.Diff(want, got))
					}
					return &FloatIterator{Points: []query.FloatPoint{
						{Name: "cpu", Time: 0 * Second, Value: 5, Aux: []interface{}{"server02"}},
						{Name: "cpu", Time: 5 * Second, Value: 3, Aux: []interface{}{"server01"}},
						{Name: "cpu", Time: 10 * Second, Value: 8, Aux: []interface{}{"server03"}},
					}}
				}
			},
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "cpu"}, Values: []interface{}{"server02"}},
				{Time: 10 * Second, Series: query.Series{Name: "cpu"}, Values: []interface{}{"server03"}},
			},
		},
		{
			Name:      "AuxiliaryFields_NonExistentField",
			Statement: `SELECT host FROM (SELECT max(value) FROM cpu GROUP BY time(5s)) WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:15Z'`,
			Fields:    map[string]influxql.DataType{"value": influxql.Float},
			MapShardsFn: func(t *testing.T, tr influxql.TimeRange) CreateIteratorFn {
				return func(ctx context.Context, m *influxql.Measurement, opt query.IteratorOptions) query.Iterator {
					return &FloatIterator{Points: []query.FloatPoint{
						{Name: "cpu", Time: 0 * Second, Value: 5},
						{Name: "cpu", Time: 5 * Second, Value: 3},
						{Name: "cpu", Time: 10 * Second, Value: 8},
					}}
				}
			},
			Rows: []query.Row(nil),
		},
		{
			Name:      "AggregateOfMath",
			Statement: `SELECT mean(percentage) FROM (SELECT value * 100.0 AS percentage FROM cpu) WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:15Z' GROUP BY time(5s)`,
			Fields:    map[string]influxql.DataType{"value": influxql.Float},
			MapShardsFn: func(t *testing.T, tr influxql.TimeRange) CreateIteratorFn {
				if got, want := tr.MinTimeNano(), 0*Second; got != want {
					t.Errorf("unexpected min time: got=%d want=%d", got, want)
				}
				if got, want := tr.MaxTimeNano(), 15*Second-1; got != want {
					t.Errorf("unexpected max time: got=%d want=%d", got, want)
				}
				return func(ctx context.Context, m *influxql.Measurement, opt query.IteratorOptions) query.Iterator {
					if got, want := m.Name, "cpu"; got != want {
						t.Errorf("unexpected source: got=%s want=%s", got, want)
					}
					if got, want := opt.Expr, influxql.Expr(nil); got != want {
						t.Errorf("unexpected expression: got=%s want=%s", got, want)
					}
					if got, want := opt.Aux, []influxql.VarRef{{Val: "value", Type: influxql.Float}}; !cmp.Equal(got, want) {
						t.Errorf("unexpected auxiliary fields:\n%s", cmp.Diff(want, got))
					}
					return &FloatIterator{Points: []query.FloatPoint{
						{Name: "cpu", Time: 0 * Second, Aux: []interface{}{0.5}},
						{Name: "cpu", Time: 2 * Second, Aux: []interface{}{1.0}},
						{Name: "cpu", Time: 5 * Second, Aux: []interface{}{0.05}},
						{Name: "cpu", Time: 8 * Second, Aux: []interface{}{0.45}},
						{Name: "cpu", Time: 12 * Second, Aux: []interface{}{0.34}},
					}}
				}
			},
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "cpu"}, Values: []interface{}{float64(75)}},
				{Time: 5 * Second, Series: query.Series{Name: "cpu"}, Values: []interface{}{float64(25)}},
				{Time: 10 * Second, Series: query.Series{Name: "cpu"}, Values: []interface{}{float64(34)}},
			},
		},
		{
			Name:      "Cast",
			Statement: `SELECT value::integer FROM (SELECT mean(value) AS value FROM cpu)`,
			Fields:    map[string]influxql.DataType{"value": influxql.Integer},
			MapShardsFn: func(t *testing.T, tr influxql.TimeRange) CreateIteratorFn {
				return func(ctx context.Context, m *influxql.Measurement, opt query.IteratorOptions) query.Iterator {
					if got, want := m.Name, "cpu"; got != want {
						t.Errorf("unexpected source: got=%s want=%s", got, want)
					}
					if got, want := opt.Expr.String(), "mean(value::integer)"; got != want {
						t.Errorf("unexpected expression: got=%s want=%s", got, want)
					}
					return &FloatIterator{Points: []query.FloatPoint{
						{Name: "cpu", Time: 0 * Second, Value: float64(20) / float64(6)},
					}}
				}
			},
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "cpu"}, Values: []interface{}{int64(3)}},
			},
		},
		{
			Name:      "CountTag",
			Statement: `SELECT count(host) FROM (SELECT value, host FROM cpu) WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:15Z'`,
			Fields: map[string]influxql.DataType{
				"value": influxql.Float,
				"host":  influxql.Tag,
			},
			MapShardsFn: func(t *testing.T, tr influxql.TimeRange) CreateIteratorFn {
				if got, want := tr.MinTimeNano(), 0*Second; got != want {
					t.Errorf("unexpected min time: got=%d want=%d", got, want)
				}
				if got, want := tr.MaxTimeNano(), 15*Second-1; got != want {
					t.Errorf("unexpected max time: got=%d want=%d", got, want)
				}
				return func(ctx context.Context, m *influxql.Measurement, opt query.IteratorOptions) query.Iterator {
					if got, want := m.Name, "cpu"; got != want {
						t.Errorf("unexpected source: got=%s want=%s", got, want)
					}
					if got, want := opt.Aux, []influxql.VarRef{
						{Val: "host", Type: influxql.Tag},
						{Val: "value", Type: influxql.Float},
					}; !cmp.Equal(got, want) {
						t.Errorf("unexpected auxiliary fields:\n%s", cmp.Diff(want, got))
					}
					return &FloatIterator{Points: []query.FloatPoint{
						{Name: "cpu", Aux: []interface{}{"server01", 5.0}},
						{Name: "cpu", Aux: []interface{}{"server02", 3.0}},
						{Name: "cpu", Aux: []interface{}{"server03", 8.0}},
					}}
				}
			},
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "cpu"}, Values: []interface{}{int64(3)}},
			},
		},
		{
			Name:      "StripTags",
			Statement: `SELECT max FROM (SELECT max(value) FROM cpu GROUP BY host) WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:15Z'`,
			Fields:    map[string]influxql.DataType{"value": influxql.Float},
			MapShardsFn: func(t *testing.T, tr influxql.TimeRange) CreateIteratorFn {
				if got, want := tr.MinTimeNano(), 0*Second; got != want {
					t.Errorf("unexpected min time: got=%d want=%d", got, want)
				}
				if got, want := tr.MaxTimeNano(), 15*Second-1; got != want {
					t.Errorf("unexpected max time: got=%d want=%d", got, want)
				}
				return func(ctx context.Context, m *influxql.Measurement, opt query.IteratorOptions) query.Iterator {
					if got, want := m.Name, "cpu"; got != want {
						t.Errorf("unexpected source: got=%s want=%s", got, want)
					}
					if got, want := opt.Expr.String(), "max(value::float)"; got != want {
						t.Errorf("unexpected expression: got=%s want=%s", got, want)
					}
					return &FloatIterator{Points: []query.FloatPoint{
						{Name: "cpu", Tags: ParseTags("host=server01"), Value: 5},
						{Name: "cpu", Tags: ParseTags("host=server02"), Value: 3},
						{Name: "cpu", Tags: ParseTags("host=server03"), Value: 8},
					}}
				}
			},
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "cpu"}, Values: []interface{}{5.0}},
				{Time: 0 * Second, Series: query.Series{Name: "cpu"}, Values: []interface{}{3.0}},
				{Time: 0 * Second, Series: query.Series{Name: "cpu"}, Values: []interface{}{8.0}},
			},
		},
	} {
		t.Run(test.Name, func(t *testing.T) {
			shardMapper := ShardMapper{
				MapShardsFn: func(sources influxql.Sources, tr influxql.TimeRange) query.ShardGroup {
					fn := test.MapShardsFn(t, tr)
					return &ShardGroup{
						Fields: test.Fields,
						CreateIteratorFn: func(ctx context.Context, m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
							return fn(ctx, m, opt), nil
						},
					}
				},
			}

			stmt := MustParseSelectStatement(test.Statement)
			stmt.OmitTime = true
			cur, err := query.Select(context.Background(), stmt, &shardMapper, query.SelectOptions{})
			if err != nil {
				t.Fatalf("unexpected parse error: %s", err)
			} else if a, err := ReadCursor(cur); err != nil {
				t.Fatalf("unexpected error: %s", err)
			} else if diff := cmp.Diff(test.Rows, a); diff != "" {
				t.Fatalf("unexpected points:\n%s", diff)
			}
		})
	}
}
