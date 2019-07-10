package query_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/models"
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
		{
			Name: "DifferentDimensionsWithSelectors",
			Statement: `SELECT sum("max_min") FROM (
							SELECT max("value") - min("value") FROM cpu GROUP BY time(30s), host
						) WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:01:00Z' GROUP BY time(30s)`,
			Fields: map[string]influxql.DataType{"value": influxql.Float},
			MapShardsFn: func(t *testing.T, tr influxql.TimeRange) CreateIteratorFn {
				if got, want := tr.MinTimeNano(), 0*Second; got != want {
					t.Errorf("unexpected min time: got=%d want=%d", got, want)
				}
				if got, want := tr.MaxTimeNano(), 60*Second-1; got != want {
					t.Errorf("unexpected max time: got=%d want=%d", got, want)
				}
				return func(ctx context.Context, m *influxql.Measurement, opt query.IteratorOptions) query.Iterator {
					if got, want := m.Name, "cpu"; got != want {
						t.Errorf("unexpected source: got=%s want=%s", got, want)
					}

					var itr query.Iterator = &FloatIterator{Points: []query.FloatPoint{
						{Name: "cpu", Tags: ParseTags("host=A"), Time: 0 * Second, Value: 2},
						{Name: "cpu", Tags: ParseTags("host=A"), Time: 10 * Second, Value: 7},
						{Name: "cpu", Tags: ParseTags("host=A"), Time: 20 * Second, Value: 3},
						{Name: "cpu", Tags: ParseTags("host=B"), Time: 0 * Second, Value: 8},
						{Name: "cpu", Tags: ParseTags("host=B"), Time: 10 * Second, Value: 3},
						{Name: "cpu", Tags: ParseTags("host=B"), Time: 20 * Second, Value: 7},
						{Name: "cpu", Tags: ParseTags("host=A"), Time: 30 * Second, Value: 2},
						{Name: "cpu", Tags: ParseTags("host=A"), Time: 40 * Second, Value: 1},
						{Name: "cpu", Tags: ParseTags("host=A"), Time: 50 * Second, Value: 9},
						{Name: "cpu", Tags: ParseTags("host=B"), Time: 30 * Second, Value: 2},
						{Name: "cpu", Tags: ParseTags("host=B"), Time: 40 * Second, Value: 2},
						{Name: "cpu", Tags: ParseTags("host=B"), Time: 50 * Second, Value: 2},
					}}
					if _, ok := opt.Expr.(*influxql.Call); ok {
						i, err := query.NewCallIterator(itr, opt)
						if err != nil {
							panic(err)
						}
						itr = i
					}
					return itr
				}
			},
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "cpu"}, Values: []interface{}{float64(10)}},
				{Time: 30 * Second, Series: query.Series{Name: "cpu"}, Values: []interface{}{float64(8)}},
			},
		},
		{
			Name:      "TimeOrderingInTheOuterQuery",
			Statement: `select * from (select last(value) from cpu group by host) order by time asc`,
			Fields:    map[string]influxql.DataType{"value": influxql.Float},
			MapShardsFn: func(t *testing.T, tr influxql.TimeRange) CreateIteratorFn {

				return func(ctx context.Context, m *influxql.Measurement, opt query.IteratorOptions) query.Iterator {
					if got, want := m.Name, "cpu"; got != want {
						t.Errorf("unexpected source: got=%s want=%s", got, want)
					}

					var itr query.Iterator = &FloatIterator{Points: []query.FloatPoint{
						{Name: "cpu", Tags: ParseTags("host=A"), Time: 0 * Second, Value: 2},
						{Name: "cpu", Tags: ParseTags("host=A"), Time: 10 * Second, Value: 7},
						{Name: "cpu", Tags: ParseTags("host=A"), Time: 20 * Second, Value: 3},
						{Name: "cpu", Tags: ParseTags("host=B"), Time: 0 * Second, Value: 8},
						{Name: "cpu", Tags: ParseTags("host=B"), Time: 10 * Second, Value: 3},
						{Name: "cpu", Tags: ParseTags("host=B"), Time: 19 * Second, Value: 7},
					}}
					if _, ok := opt.Expr.(*influxql.Call); ok {
						i, err := query.NewCallIterator(itr, opt)
						if err != nil {
							panic(err)
						}
						itr = i
					}
					return itr
				}
			},
			Rows: []query.Row{
				{Time: 19 * Second, Series: query.Series{Name: "cpu"}, Values: []interface{}{"B", float64(7)}},
				{Time: 20 * Second, Series: query.Series{Name: "cpu"}, Values: []interface{}{"A", float64(3)}},
			},
		},
		{
			Name:      "TimeZone",
			Statement: `SELECT * FROM (SELECT * FROM cpu WHERE time >= '2019-04-17 09:00:00' and time < '2019-04-17 10:00:00' TZ('America/Chicago'))`,
			Fields:    map[string]influxql.DataType{"value": influxql.Float},
			MapShardsFn: func(t *testing.T, tr influxql.TimeRange) CreateIteratorFn {
				return func(ctx context.Context, m *influxql.Measurement, opt query.IteratorOptions) query.Iterator {
					if got, want := time.Unix(0, opt.StartTime).UTC(), mustParseTime("2019-04-17T14:00:00Z"); !got.Equal(want) {
						t.Errorf("unexpected min time: got=%q want=%q", got, want)
					}
					if got, want := time.Unix(0, opt.EndTime).UTC(), mustParseTime("2019-04-17T15:00:00Z").Add(-1); !got.Equal(want) {
						t.Errorf("unexpected max time: got=%q want=%q", got, want)
					}
					return &FloatIterator{}
				}
			},
		},
		{
			Name:      "DifferentDimensionsOrderByDesc",
			Statement: `SELECT value, mytag FROM (SELECT last(value) AS value FROM testing GROUP BY mytag) ORDER BY desc`,
			Fields:    map[string]influxql.DataType{"value": influxql.Float},
			MapShardsFn: func(t *testing.T, tr influxql.TimeRange) CreateIteratorFn {
				return func(ctx context.Context, m *influxql.Measurement, opt query.IteratorOptions) query.Iterator {
					if got, want := m.Name, "testing"; got != want {
						t.Errorf("unexpected source: got=%s want=%s", got, want)
					}

					if opt.Ascending {
						t.Error("expected iterator to be descending, not ascending")
					}

					var itr query.Iterator = &FloatIterator{Points: []query.FloatPoint{
						{Name: "testing", Tags: ParseTags("mytag=c"), Time: mustParseTime("2019-06-25T22:36:20.93605779Z").UnixNano(), Value: 2},
						{Name: "testing", Tags: ParseTags("mytag=c"), Time: mustParseTime("2019-06-25T22:36:20.671604877Z").UnixNano(), Value: 2},
						{Name: "testing", Tags: ParseTags("mytag=c"), Time: mustParseTime("2019-06-25T22:36:20.255794481Z").UnixNano(), Value: 2},
						{Name: "testing", Tags: ParseTags("mytag=b"), Time: mustParseTime("2019-06-25T22:36:18.176662543Z").UnixNano(), Value: 2},
						{Name: "testing", Tags: ParseTags("mytag=b"), Time: mustParseTime("2019-06-25T22:36:17.815979113Z").UnixNano(), Value: 2},
						{Name: "testing", Tags: ParseTags("mytag=b"), Time: mustParseTime("2019-06-25T22:36:17.265031598Z").UnixNano(), Value: 2},
						{Name: "testing", Tags: ParseTags("mytag=a"), Time: mustParseTime("2019-06-25T22:36:15.144253616Z").UnixNano(), Value: 2},
						{Name: "testing", Tags: ParseTags("mytag=a"), Time: mustParseTime("2019-06-25T22:36:14.719167205Z").UnixNano(), Value: 2},
						{Name: "testing", Tags: ParseTags("mytag=a"), Time: mustParseTime("2019-06-25T22:36:13.711721316Z").UnixNano(), Value: 2},
					}}
					if _, ok := opt.Expr.(*influxql.Call); ok {
						i, err := query.NewCallIterator(itr, opt)
						if err != nil {
							panic(err)
						}
						itr = i
					}
					return itr
				}
			},
			Rows: []query.Row{
				{Time: mustParseTime("2019-06-25T22:36:20.93605779Z").UnixNano(), Series: query.Series{Name: "testing"}, Values: []interface{}{float64(2), "c"}},
				{Time: mustParseTime("2019-06-25T22:36:18.176662543Z").UnixNano(), Series: query.Series{Name: "testing"}, Values: []interface{}{float64(2), "b"}},
				{Time: mustParseTime("2019-06-25T22:36:15.144253616Z").UnixNano(), Series: query.Series{Name: "testing"}, Values: []interface{}{float64(2), "a"}},
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

type openAuthorizer struct{}

func (*openAuthorizer) AuthorizeDatabase(p influxql.Privilege, name string) bool    { return true }
func (*openAuthorizer) AuthorizeQuery(database string, query *influxql.Query) error { return nil }
func (*openAuthorizer) AuthorizeSeriesRead(database string, measurement []byte, tags models.Tags) bool {
	return true
}
func (*openAuthorizer) AuthorizeSeriesWrite(database string, measurement []byte, tags models.Tags) bool {
	return true
}

// Ensure that the subquery gets passed the query authorizer.
func TestSubquery_Authorizer(t *testing.T) {
	auth := &openAuthorizer{}
	shardMapper := ShardMapper{
		MapShardsFn: func(sources influxql.Sources, tr influxql.TimeRange) query.ShardGroup {
			return &ShardGroup{
				Fields: map[string]influxql.DataType{
					"value": influxql.Float,
				},
				CreateIteratorFn: func(ctx context.Context, m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
					if opt.Authorizer != auth {
						t.Errorf("query authorizer has not been set")
					}
					return nil, nil
				},
			}
		},
	}

	stmt := MustParseSelectStatement(`SELECT max(value) FROM (SELECT value FROM cpu)`)
	cur, err := query.Select(context.Background(), stmt, &shardMapper, query.SelectOptions{
		Authorizer: auth,
	})
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	cur.Close()
}

// Ensure that the subquery gets passed the max series limit.
func TestSubquery_MaxSeriesN(t *testing.T) {
	shardMapper := ShardMapper{
		MapShardsFn: func(sources influxql.Sources, tr influxql.TimeRange) query.ShardGroup {
			return &ShardGroup{
				Fields: map[string]influxql.DataType{
					"value": influxql.Float,
				},
				CreateIteratorFn: func(ctx context.Context, m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
					if opt.MaxSeriesN != 1000 {
						t.Errorf("max series limit has not been set")
					}
					return nil, nil
				},
			}
		},
	}

	stmt := MustParseSelectStatement(`SELECT max(value) FROM (SELECT value FROM cpu)`)
	cur, err := query.Select(context.Background(), stmt, &shardMapper, query.SelectOptions{
		MaxSeriesN: 1000,
	})
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	cur.Close()
}
