package storage_test

import (
	"testing"

	"github.com/influxdata/influxdb/pkg/testing/assert"
	"github.com/influxdata/influxdb/services/storage"
	"github.com/influxdata/influxql"
)

func TestPredicateToExprString(t *testing.T) {
	cases := []struct {
		n string
		r *storage.Predicate
		e string
	}{
		{
			n: "returns [none] for nil",
			r: nil,
			e: "[none]",
		},
		{
			n: "logical AND",
			r: &storage.Predicate{
				Root: &storage.Node{
					NodeType: storage.NodeTypeLogicalExpression,
					Value:    &storage.Node_Logical_{Logical: storage.LogicalAnd},
					Children: []*storage.Node{
						{
							NodeType: storage.NodeTypeComparisonExpression,
							Value:    &storage.Node_Comparison_{Comparison: storage.ComparisonEqual},
							Children: []*storage.Node{
								{NodeType: storage.NodeTypeTagRef, Value: &storage.Node_TagRefValue{TagRefValue: "host"}},
								{NodeType: storage.NodeTypeLiteral, Value: &storage.Node_StringValue{StringValue: "host1"}},
							},
						},
						{
							NodeType: storage.NodeTypeComparisonExpression,
							Value:    &storage.Node_Comparison_{Comparison: storage.ComparisonRegex},
							Children: []*storage.Node{
								{NodeType: storage.NodeTypeTagRef, Value: &storage.Node_TagRefValue{TagRefValue: "region"}},
								{NodeType: storage.NodeTypeLiteral, Value: &storage.Node_RegexValue{RegexValue: "^us-west"}},
							},
						},
					},
				},
			},
			e: `'host' = "host1" AND 'region' =~ /^us-west/`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.n, func(t *testing.T) {
			assert.Equal(t, storage.PredicateToExprString(tc.r), tc.e)
		})
	}
}

func TestNodeToExpr(t *testing.T) {
	cases := []struct {
		n string
		r *storage.Node
		m map[string]string
		e string
	}{
		{
			n: "simple expression",
			r: &storage.Node{
				NodeType: storage.NodeTypeComparisonExpression,
				Value:    &storage.Node_Comparison_{Comparison: storage.ComparisonEqual},
				Children: []*storage.Node{
					{NodeType: storage.NodeTypeTagRef, Value: &storage.Node_TagRefValue{TagRefValue: "host"}},
					{NodeType: storage.NodeTypeLiteral, Value: &storage.Node_StringValue{StringValue: "host1"}},
				},
			},
			e: `host = 'host1'`,
		},
		{
			n: "logical AND with regex",
			r: &storage.Node{
				NodeType: storage.NodeTypeLogicalExpression,
				Value:    &storage.Node_Logical_{Logical: storage.LogicalAnd},
				Children: []*storage.Node{
					{
						NodeType: storage.NodeTypeComparisonExpression,
						Value:    &storage.Node_Comparison_{Comparison: storage.ComparisonEqual},
						Children: []*storage.Node{
							{NodeType: storage.NodeTypeTagRef, Value: &storage.Node_TagRefValue{TagRefValue: "host"}},
							{NodeType: storage.NodeTypeLiteral, Value: &storage.Node_StringValue{StringValue: "host1"}},
						},
					},
					{
						NodeType: storage.NodeTypeComparisonExpression,
						Value:    &storage.Node_Comparison_{Comparison: storage.ComparisonRegex},
						Children: []*storage.Node{
							{NodeType: storage.NodeTypeTagRef, Value: &storage.Node_TagRefValue{TagRefValue: "region"}},
							{NodeType: storage.NodeTypeLiteral, Value: &storage.Node_RegexValue{RegexValue: "^us-west"}},
						},
					},
				},
			},
			e: `host = 'host1' AND region =~ /^us-west/`,
		},
		{
			n: "remap _measurement -> _name",
			r: &storage.Node{
				NodeType: storage.NodeTypeComparisonExpression,
				Value:    &storage.Node_Comparison_{Comparison: storage.ComparisonEqual},
				Children: []*storage.Node{
					{NodeType: storage.NodeTypeTagRef, Value: &storage.Node_TagRefValue{TagRefValue: "_measurement"}},
					{NodeType: storage.NodeTypeLiteral, Value: &storage.Node_StringValue{StringValue: "foo"}},
				},
			},
			m: map[string]string{"_measurement": "_name"},
			e: `_name = 'foo'`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.n, func(t *testing.T) {
			expr, err := storage.NodeToExpr(tc.r, tc.m)
			assert.NoError(t, err)
			assert.Equal(t, expr.String(), tc.e)
		})
	}
}

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
	node := &storage.Node{
		NodeType: storage.NodeTypeLogicalExpression,
		Value:    &storage.Node_Logical_{Logical: storage.LogicalAnd},
		Children: []*storage.Node{
			{
				NodeType: storage.NodeTypeComparisonExpression,
				Value:    &storage.Node_Comparison_{Comparison: storage.ComparisonEqual},
				Children: []*storage.Node{
					{NodeType: storage.NodeTypeTagRef, Value: &storage.Node_TagRefValue{TagRefValue: "host"}},
					{NodeType: storage.NodeTypeLiteral, Value: &storage.Node_StringValue{StringValue: "host1"}},
				},
			},
			{
				NodeType: storage.NodeTypeComparisonExpression,
				Value:    &storage.Node_Comparison_{Comparison: storage.ComparisonRegex},
				Children: []*storage.Node{
					{NodeType: storage.NodeTypeTagRef, Value: &storage.Node_TagRefValue{TagRefValue: "_field"}},
					{NodeType: storage.NodeTypeLiteral, Value: &storage.Node_RegexValue{RegexValue: "^us-west"}},
				},
			},
			{
				NodeType: storage.NodeTypeComparisonExpression,
				Value:    &storage.Node_Comparison_{Comparison: storage.ComparisonEqual},
				Children: []*storage.Node{
					{NodeType: storage.NodeTypeTagRef, Value: &storage.Node_TagRefValue{TagRefValue: "$"}},
					{NodeType: storage.NodeTypeLiteral, Value: &storage.Node_FloatValue{FloatValue: 0.5}},
				},
			},
		},
	}

	expr, err := storage.NodeToExpr(node, nil)
	assert.NoError(t, err, "NodeToExpr failed")
	assert.Equal(t, expr.String(), `host = 'host1' AND _field =~ /^us-west/ AND "$" = 0.500`)

	expr = storage.RewriteExprRemoveFieldKeyAndValue(expr)
	assert.Equal(t, expr.String(), `host = 'host1' AND true AND true`)

	expr = influxql.Reduce(expr, mapValuer{"host": "host1"})
	assert.Equal(t, expr.String(), `true`)
}

type mapValuer map[string]string

var _ influxql.Valuer = mapValuer(nil)

func (vs mapValuer) Value(key string) (interface{}, bool) {
	v, ok := vs[key]
	return v, ok
}
