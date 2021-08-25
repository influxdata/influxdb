package storage_test

import (
	"testing"

	"github.com/influxdata/influxdb/v2/pkg/testing/assert"
	"github.com/influxdata/influxdb/v2/storage/reads"
	"github.com/influxdata/influxdb/v2/storage/reads/datatypes"
	"github.com/influxdata/influxdb/v2/v1/services/storage"
	"github.com/influxdata/influxql"
	"github.com/stretchr/testify/require"
)

func TestMeasurementOptimization(t *testing.T) {
	cases := []struct {
		expr  influxql.Expr
		name  string
		ok    bool
		names []string
	}{
		{
			expr:  influxql.MustParseExpr(`_name = 'm0'`),
			name:  "single measurement",
			ok:    true,
			names: []string{"m0"},
		},
		{
			expr:  influxql.MustParseExpr(`_something = 'f' AND _name = 'm0'`),
			name:  "single measurement with AND",
			ok:    true,
			names: []string{"m0"},
		},
		{
			expr:  influxql.MustParseExpr(`_something = 'f' AND (a =~ /x0/ AND _name = 'm0')`),
			name:  "single measurement with multiple AND",
			ok:    true,
			names: []string{"m0"},
		},
		{
			expr:  influxql.MustParseExpr(`_name = 'm0' OR _name = 'm1' OR _name = 'm2'`),
			name:  "multiple measurements alone",
			ok:    true,
			names: []string{"m0", "m1", "m2"},
		},
		{
			expr:  influxql.MustParseExpr(`(_name = 'm0' OR _name = 'm1' OR _name = 'm2') AND (_field = 'foo' OR _field = 'bar' OR _field = 'qux')`),
			name:  "multiple measurements combined",
			ok:    true,
			names: []string{"m0", "m1", "m2"},
		},
		{
			expr:  influxql.MustParseExpr(`(_name = 'm0' OR (_name = 'm1' OR _name = 'm2')) AND tag1 != 'foo'`),
			name:  "parens in expression",
			ok:    true,
			names: []string{"m0", "m1", "m2"},
		},
		{
			expr:  influxql.MustParseExpr(`(tag1 != 'foo' OR tag2 = 'bar') AND (_name = 'm0' OR _name = 'm1' OR _name = 'm2') AND (_field = 'val1' OR _field = 'val2')`),
			name:  "multiple AND",
			ok:    true,
			names: []string{"m0", "m1", "m2"},
		},
		{
			expr:  influxql.MustParseExpr(`_name = 'm0' OR tag1 != 'foo'`),
			name:  "single measurement with OR",
			ok:    false,
			names: nil,
		},
		{
			expr:  influxql.MustParseExpr(`_name != 'm0' AND tag1 != 'foo'`),
			name:  "single measurement with non-equal",
			ok:    false,
			names: nil,
		},
		{
			expr:  influxql.MustParseExpr(`_name = 'm0' AND _name != 'm1' AND tag1 != 'foo'`),
			name:  "multi-measurement, top level, equal and non-equal",
			ok:    false,
			names: nil,
		},
		{
			expr:  influxql.MustParseExpr(`(_name = 'm0' OR _name != 'm1' OR _name = 'm2') AND (_field = 'foo' OR _field = 'bar' OR _field = 'qux')`),
			name:  "multiple measurements with non-equal",
			ok:    false,
			names: nil,
		},
		{
			expr:  influxql.MustParseExpr(`tag1 = 'foo' AND tag2 = 'bar'`),
			name:  "no measurements - multiple tags",
			ok:    false,
			names: nil,
		},
		{
			expr:  influxql.MustParseExpr(`_field = 'foo'`),
			name:  "no measurements - single field",
			ok:    false,
			names: nil,
		},
		{
			expr:  influxql.MustParseExpr(`(_name = 'm0' OR _name = 'm1' OR _name = 'm2') AND (tag1 != 'foo' OR _name = 'm1')`),
			name:  "measurements on both sides",
			ok:    false,
			names: nil,
		},
		{
			expr:  influxql.MustParseExpr(`(tag1 != 'foo' OR _name = 'm4') AND (_name = 'm0' OR _name = 'm1' OR _name = 'm2') AND (_field = 'val1' OR _name = 'm3')`),
			name:  "multiple AND - dispersed measurements",
			ok:    false,
			names: nil,
		},
		{
			expr:  influxql.MustParseExpr(`(_name = 'm0' OR _name = 'm1' AND _name = 'm2') AND tag1 != 'foo'`),
			name:  "measurements with AND",
			ok:    false,
			names: nil,
		},
		{
			expr:  influxql.MustParseExpr(`(_name = 'm0' OR _name = 'm1' OR _name = 'm2') OR (tag1 != 'foo' OR _name = 'm1')`),
			name:  "top level is not AND",
			ok:    false,
			names: nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			names, ok := storage.MeasurementOptimization(tc.expr)
			require.Equal(t, tc.names, names)
			require.Equal(t, tc.ok, ok)
		})
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
