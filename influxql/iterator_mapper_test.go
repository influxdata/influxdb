package influxql_test

import (
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/pkg/deep"
)

func TestIteratorMapper(t *testing.T) {
	val1itr := &FloatIterator{Points: []influxql.FloatPoint{
		{Name: "cpu", Tags: ParseTags("host=A"), Time: 0, Value: 1},
		{Name: "cpu", Tags: ParseTags("host=A"), Time: 5, Value: 3},
		{Name: "cpu", Tags: ParseTags("host=B"), Time: 2, Value: 2},
		{Name: "cpu", Tags: ParseTags("host=B"), Time: 8, Value: 8},
	}}

	val2itr := &StringIterator{Points: []influxql.StringPoint{
		{Name: "cpu", Tags: ParseTags("host=A"), Time: 0, Value: "a"},
		{Name: "cpu", Tags: ParseTags("host=A"), Time: 5, Value: "c"},
		{Name: "cpu", Tags: ParseTags("host=B"), Time: 2, Value: "b"},
		{Name: "cpu", Tags: ParseTags("host=B"), Time: 8, Value: "h"},
	}}
	inputs := []influxql.Iterator{val1itr, val2itr}

	opt := influxql.IteratorOptions{
		Ascending: true,
		Aux: []influxql.VarRef{
			{Val: "val1", Type: influxql.Float},
			{Val: "val2", Type: influxql.String},
		},
	}
	itr := influxql.NewIteratorMapper(inputs, []influxql.IteratorMap{
		influxql.FieldMap(0),
		influxql.FieldMap(1),
		influxql.TagMap("host"),
	}, opt)
	if a, err := Iterators([]influxql.Iterator{itr}).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if !deep.Equal(a, [][]influxql.Point{
		{&influxql.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 0, Aux: []interface{}{float64(1), "a", "A"}}},
		{&influxql.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 5, Aux: []interface{}{float64(3), "c", "A"}}},
		{&influxql.FloatPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 2, Aux: []interface{}{float64(2), "b", "B"}}},
		{&influxql.FloatPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 8, Aux: []interface{}{float64(8), "h", "B"}}},
	}) {
		t.Errorf("unexpected points: %s", spew.Sdump(a))
	}

	for i, input := range inputs {
		switch input := input.(type) {
		case *FloatIterator:
			if !input.Closed {
				t.Errorf("iterator %d not closed", i)
			}
		case *StringIterator:
			if !input.Closed {
				t.Errorf("iterator %d not closed", i)
			}
		}
	}
}
