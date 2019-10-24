package query_test

import (
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/influxdata/influxdb/pkg/deep"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxql"
)

func TestIteratorMapper(t *testing.T) {
	cur := query.RowCursor([]query.Row{
		{
			Time: 0,
			Series: query.Series{
				Name: "cpu",
				Tags: ParseTags("host=A"),
			},
			Values: []interface{}{float64(1), "a"},
		},
		{
			Time: 5,
			Series: query.Series{
				Name: "cpu",
				Tags: ParseTags("host=A"),
			},
			Values: []interface{}{float64(3), "c"},
		},
		{
			Time: 2,
			Series: query.Series{
				Name: "cpu",
				Tags: ParseTags("host=B"),
			},
			Values: []interface{}{float64(2), "b"},
		},
		{
			Time: 8,
			Series: query.Series{
				Name: "cpu",
				Tags: ParseTags("host=B"),
			},
			Values: []interface{}{float64(8), "h"},
		},
	}, []influxql.VarRef{
		{Val: "val1", Type: influxql.Float},
		{Val: "val2", Type: influxql.String},
	})

	opt := query.IteratorOptions{
		Ascending: true,
		Aux: []influxql.VarRef{
			{Val: "val1", Type: influxql.Float},
			{Val: "val2", Type: influxql.String},
		},
		Dimensions: []string{"host"},
	}
	itr := query.NewIteratorMapper(cur, nil, []query.IteratorMap{
		query.FieldMap{Index: 0},
		query.FieldMap{Index: 1},
		query.TagMap("host"),
	}, opt)
	if a, err := Iterators([]query.Iterator{itr}).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if !deep.Equal(a, [][]query.Point{
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 0, Aux: []interface{}{float64(1), "a", "A"}}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 5, Aux: []interface{}{float64(3), "c", "A"}}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 2, Aux: []interface{}{float64(2), "b", "B"}}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 8, Aux: []interface{}{float64(8), "h", "B"}}},
	}) {
		t.Errorf("unexpected points: %s", spew.Sdump(a))
	}
}
