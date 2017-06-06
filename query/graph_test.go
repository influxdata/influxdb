package query_test

import (
	"testing"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/query"
)

func TestNode_Type(t *testing.T) {
	for _, tt := range []struct {
		name string
		typ  influxql.DataType
		fn   func() query.Node
	}{
		{
			name: "a single data type",
			typ:  influxql.Integer,
			fn: func() query.Node {
				input := &query.AuxiliaryField{
					Ref: &influxql.VarRef{Type: influxql.Integer},
				}
				output := &query.FunctionCall{Name: "min"}
				input.Output, output.Input = query.AddEdge(input, output)
				return output
			},
		},
		{
			name: `count() with a float`,
			typ:  influxql.Integer,
			fn: func() query.Node {
				input := &query.AuxiliaryField{
					Ref: &influxql.VarRef{Type: influxql.Float},
				}
				output := &query.FunctionCall{Name: "count"}
				input.Output, output.Input = query.AddEdge(input, output)
				return output
			},
		},
		{
			name: `mean() with an integer`,
			typ:  influxql.Float,
			fn: func() query.Node {
				input := &query.AuxiliaryField{
					Ref: &influxql.VarRef{Type: influxql.Integer},
				}
				output := &query.FunctionCall{Name: "mean"}
				input.Output, output.Input = query.AddEdge(input, output)
				return output
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			n := tt.fn()
			if typ := n.Type(); typ != tt.typ {
				t.Fatalf("unexpected data type: have=%s want=%s", typ, tt.typ)
			}
		})
	}
}
