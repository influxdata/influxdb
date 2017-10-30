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
			n: "locical AND with regex",
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
	}

	for _, tc := range cases {
		t.Run(tc.n, func(t *testing.T) {
			expr, err := storage.NodeToExpr(tc.r)
			assert.NoError(t, err)
			assert.Equal(t, expr.String(), tc.e)
		})
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

	expr, err := storage.NodeToExpr(node)
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
