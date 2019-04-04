package storage_test

import (
	"testing"

	"github.com/influxdata/influxdb/pkg/testing/assert"
	"github.com/influxdata/influxdb/services/storage"
	"github.com/influxdata/influxdb/storage/reads"
	"github.com/influxdata/influxdb/storage/reads/datatypes"
	"github.com/influxdata/influxql"
)

func TestHasSingleMeasurementNoOR(t *testing.T) {
	cases := []struct {
		expr influxql.Expr
		name string
		ok   bool
	}{
		{
			expr: influxql.MustParseExpr(`_name = 'm0'`),
			name: "m0",
			ok:   true,
		},
		{
			expr: influxql.MustParseExpr(`_something = 'f' AND _name = 'm0'`),
			name: "m0",
			ok:   true,
		},
		{
			expr: influxql.MustParseExpr(`_something = 'f' AND (a =~ /x0/ AND _name = 'm0')`),
			name: "m0",
			ok:   true,
		},
		{
			expr: influxql.MustParseExpr(`tag1 != 'foo'`),
			ok:   false,
		},
		{
			expr: influxql.MustParseExpr(`_name = 'm0' OR tag1 != 'foo'`),
			ok:   false,
		},
		{
			expr: influxql.MustParseExpr(`_name = 'm0' AND tag1 != 'foo' AND _name = 'other'`),
			ok:   false,
		},
		{
			expr: influxql.MustParseExpr(`_name = 'm0' AND tag1 != 'foo' OR _name = 'other'`),
			ok:   false,
		},
		{
			expr: influxql.MustParseExpr(`_name = 'm0' AND (tag1 != 'foo' OR tag2 = 'other')`),
			ok:   false,
		},
		{
			expr: influxql.MustParseExpr(`(tag1 != 'foo' OR tag2 = 'other') OR _name = 'm0'`),
			ok:   false,
		},
	}

	for _, tc := range cases {
		name, ok := storage.HasSingleMeasurementNoOR(tc.expr)
		if ok != tc.ok {
			t.Fatalf("got %q, %v for expression %q, expected %q, %v", name, ok, tc.expr, tc.name, tc.ok)
		}

		if ok && name != tc.name {
			t.Fatalf("got %q, %v for expression %q, expected %q, %v", name, ok, tc.expr, tc.name, tc.ok)
		}
	}
}

func TestRewriteExprRemoveFieldKeyAndValue(t *testing.T) {
	node := &datatypes.Node{
		NodeType: datatypes.NodeTypeLogicalExpression,
		Value:    &datatypes.Node_Logical_{Logical: datatypes.LogicalAnd},
		Children: []*datatypes.Node{
			{
				NodeType: datatypes.NodeTypeComparisonExpression,
				Value:    &datatypes.Node_Comparison_{Comparison: datatypes.ComparisonEqual},
				Children: []*datatypes.Node{
					{NodeType: datatypes.NodeTypeTagRef, Value: &datatypes.Node_TagRefValue{TagRefValue: "host"}},
					{NodeType: datatypes.NodeTypeLiteral, Value: &datatypes.Node_StringValue{StringValue: "host1"}},
				},
			},
			{
				NodeType: datatypes.NodeTypeComparisonExpression,
				Value:    &datatypes.Node_Comparison_{Comparison: datatypes.ComparisonRegex},
				Children: []*datatypes.Node{
					{NodeType: datatypes.NodeTypeTagRef, Value: &datatypes.Node_TagRefValue{TagRefValue: "_field"}},
					{NodeType: datatypes.NodeTypeLiteral, Value: &datatypes.Node_RegexValue{RegexValue: "^us-west"}},
				},
			},
			{
				NodeType: datatypes.NodeTypeComparisonExpression,
				Value:    &datatypes.Node_Comparison_{Comparison: datatypes.ComparisonEqual},
				Children: []*datatypes.Node{
					{NodeType: datatypes.NodeTypeFieldRef, Value: &datatypes.Node_FieldRefValue{FieldRefValue: "$"}},
					{NodeType: datatypes.NodeTypeLiteral, Value: &datatypes.Node_FloatValue{FloatValue: 0.5}},
				},
			},
		},
	}

	expr, err := reads.NodeToExpr(node, nil)
	assert.NoError(t, err, "NodeToExpr failed")
	assert.Equal(t, expr.String(), `host::tag = 'host1' AND _field::tag =~ /^us-west/ AND "$" = 0.500`)

	expr = storage.RewriteExprRemoveFieldKeyAndValue(expr)
	assert.Equal(t, expr.String(), `host::tag = 'host1' AND true AND true`)

	expr = influxql.Reduce(expr, mapValuer{"host": "host1"})
	assert.Equal(t, expr.String(), `true`)
}

type mapValuer map[string]string

var _ influxql.Valuer = mapValuer(nil)

func (vs mapValuer) Value(key string) (interface{}, bool) {
	v, ok := vs[key]
	return v, ok
}
