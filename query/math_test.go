package query_test

import (
	"testing"

	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxql"
)

func TestMath_TypeMapper(t *testing.T) {
	for _, tt := range []struct {
		s   string
		typ influxql.DataType
	}{
		{s: `sin(f::float)`, typ: influxql.Float},
		{s: `sin(i::integer)`, typ: influxql.Float},
		{s: `sin(u::unsigned)`, typ: influxql.Float},
		{s: `cos(f::float)`, typ: influxql.Float},
		{s: `cos(i::integer)`, typ: influxql.Float},
		{s: `cos(u::unsigned)`, typ: influxql.Float},
		{s: `tan(f::float)`, typ: influxql.Float},
		{s: `tan(i::integer)`, typ: influxql.Float},
		{s: `tan(u::unsigned)`, typ: influxql.Float},
	} {
		t.Run(tt.s, func(t *testing.T) {
			expr := MustParseExpr(tt.s)

			typmap := influxql.TypeValuerEval{
				TypeMapper: query.MathTypeMapper{},
			}
			if got, err := typmap.EvalType(expr); err != nil {
				t.Errorf("unexpected error: %s", err)
			} else if want := tt.typ; got != want {
				t.Errorf("unexpected type:\n\t-: \"%s\"\n\t+: \"%s\"", want, got)
			}
		})
	}
}
