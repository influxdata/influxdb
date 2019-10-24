package storage

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxql"
)

func exprEqual(x, y influxql.Expr) bool {
	if x == nil {
		if y == nil {
			return true
		}
		return false
	}

	if y == nil {
		return false
	}

	return x.String() == y.String()
}

func TestSeriesCursorValuer(t *testing.T) {
	tests := []struct {
		n    string
		m    string
		expr string
		exp  string
	}{
		{
			n:    "equals name",
			m:    "cpu,_field=foo",
			expr: `"_name"::tag = 'cpu' AND "$"::tag = 3`,
			exp:  `"$"::tag = 3`,
		},
		{
			n:    "not equals name",
			m:    "cpu,_field=foo",
			expr: `"_name"::tag = 'mem' AND "$"::tag = 3`,
			exp:  `false`,
		},
		{
			n:    "equals tag",
			m:    "cpu,_field=foo,tag0=val0",
			expr: `"tag0"::tag = 'val0' AND "$"::tag = 3`,
			exp:  `"$"::tag = 3`,
		},
		{
			n:    "not equals tag",
			m:    "cpu,_field=foo,tag0=val0",
			expr: `"tag0"::tag = 'val1' AND "$"::tag = 3`,
			exp:  `false`,
		},
		{
			n:    "missing tag",
			m:    "cpu,_field=foo,tag0=val0",
			expr: `"tag1"::tag = 'val1' AND "$"::tag = 3`,
			exp:  `false`,
		},
		{
			n:    "equals field",
			m:    "cpu,_field=foo,tag0=val0",
			expr: `"tag0"::tag = 'val1' AND "$"::tag = 3`,
			exp:  `false`,
		},
		{
			n:    "not equals field",
			m:    "cpu,_field=foo,tag0=val0",
			expr: `"_field"::tag = 'bar' AND "$"::tag = 3`,
			exp:  `false`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.n, func(t *testing.T) {
			var sc indexSeriesCursor
			sc.row.Name, sc.row.SeriesTags = models.ParseKeyBytes([]byte(tc.m))
			sc.field.n = sc.row.SeriesTags.GetString(fieldKey)
			sc.row.SeriesTags.Delete(fieldKeyBytes)

			expr, err := influxql.ParseExpr(tc.expr)
			if err != nil {
				t.Fatalf("unable to parse input expression %q, %v", tc.expr, err)
			}
			exp, err := influxql.ParseExpr(tc.exp)
			if err != nil {
				t.Fatalf("unable to parse expected expression %q, %v", tc.exp, err)
			}

			if got := influxql.Reduce(expr, &sc); !cmp.Equal(got, exp, cmp.Comparer(exprEqual)) {
				t.Errorf("unexpected result from Reduce, -got/+exp\n%s", cmp.Diff(got, exp))
			}
		})
	}
}
