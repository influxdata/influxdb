package influxdb

// This file is run within the "influxdb" package and allows for internal unit tests.

import (
	"reflect"
	"testing"

	"github.com/influxdb/influxdb/influxql"
)

// Ensure a measurement can return a set of unique tag values specified by an expression.
func TestMeasurement_uniqueTagValues(t *testing.T) {
	// Create a measurement to run against.
	m := NewMeasurement("cpu")
	m.createFieldIfNotExists("value", influxql.Number)

	for i, tt := range []struct {
		expr   string
		values map[string][]string
	}{
		{expr: `1`, values: map[string][]string{}},
		{expr: `foo = 'bar'`, values: map[string][]string{"foo": {"bar"}}},
		{expr: `(region = 'us-west' AND value > 10) OR ('us-east' = region AND value > 20) OR (host = 'serverA' AND value > 30)`, values: map[string][]string{"region": {"us-east", "us-west"}, "host": {"serverA"}}},
	} {
		// Extract unique tag values from the expression.
		values := m.uniqueTagValues(MustParseExpr(tt.expr))
		if !reflect.DeepEqual(tt.values, values) {
			t.Errorf("%d. %s: mismatch: exp=%+v, got=%+v", i, tt.expr, tt.values, values)
		}
	}
}

// Ensure a measurement can expand an expression for all possible tag values used.
func TestMeasurement_expandExpr(t *testing.T) {
	m := NewMeasurement("cpu")
	m.createFieldIfNotExists("value", influxql.Number)

	type tagSetExprString struct {
		tagExpr []tagExpr
		expr    string
	}

	for i, tt := range []struct {
		expr  string
		exprs []tagSetExprString
	}{
		// Single tag key, single value.
		{
			expr: `region = 'us-east' AND value > 10`,
			exprs: []tagSetExprString{
				{tagExpr: []tagExpr{{"region", []string{"us-east"}, influxql.EQ}}, expr: `value > 10.000`},
			},
		},

		// Single tag key, multiple values.
		{
			expr: `(region = 'us-east' AND value > 10) OR (region = 'us-west' AND value > 20)`,
			exprs: []tagSetExprString{
				{tagExpr: []tagExpr{{"region", []string{"us-east"}, influxql.EQ}}, expr: `value > 10.000`},
				{tagExpr: []tagExpr{{"region", []string{"us-west"}, influxql.EQ}}, expr: `value > 20.000`},
			},
		},

		// Multiple tag keys, multiple values.
		{
			expr: `(region = 'us-east' AND value > 10) OR ((host = 'serverA' OR host = 'serverB') AND value > 20)`,
			exprs: []tagSetExprString{
				{tagExpr: []tagExpr{{key: "host", values: []string{"serverA"}, op: influxql.EQ}, {key: "region", values: []string{"us-east"}, op: influxql.EQ}}, expr: "(value > 10.000) OR (value > 20.000)"},
				{tagExpr: []tagExpr{{key: "host", values: []string{"serverA"}, op: influxql.EQ}, {key: "region", values: []string{"us-east"}, op: influxql.NEQ}}, expr: "value > 20.000"},
				{tagExpr: []tagExpr{{key: "host", values: []string{"serverB"}, op: influxql.EQ}, {key: "region", values: []string{"us-east"}, op: influxql.EQ}}, expr: "(value > 10.000) OR (value > 20.000)"},
				{tagExpr: []tagExpr{{key: "host", values: []string{"serverB"}, op: influxql.EQ}, {key: "region", values: []string{"us-east"}, op: influxql.NEQ}}, expr: "value > 20.000"},
				{tagExpr: []tagExpr{{key: "host", values: []string{"serverA", "serverB"}, op: influxql.NEQ}, {key: "region", values: []string{"us-east"}, op: influxql.EQ}}, expr: "value > 10.000"},
			},
		},
	} {
		// Expand out an expression to all possible expressions based on tag values.
		tagExprs := m.expandExpr(MustParseExpr(tt.expr))

		// Convert to intermediate representation.
		var a []tagSetExprString
		for _, tagExpr := range tagExprs {
			a = append(a, tagSetExprString{tagExpr: tagExpr.values, expr: tagExpr.expr.String()})
		}

		// Validate that the expanded expressions are what we expect.
		if !reflect.DeepEqual(tt.exprs, a) {
			t.Errorf("%d. %s: mismatch:\n\nexp=%#v\n\ngot=%#v\n\ns", i, tt.expr, tt.exprs, a)
		}
	}
}

// MustParseExpr parses an expression string and returns its AST representation.
func MustParseExpr(s string) influxql.Expr {
	expr, err := influxql.ParseExpr(s)
	if err != nil {
		panic(err.Error())
	}
	return expr
}

func strref(s string) *string {
	return &s
}
