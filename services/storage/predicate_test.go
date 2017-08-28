package storage_test

import (
	"testing"

	"encoding/json"

	"github.com/influxdata/influxdb/services/storage"
)

func TestWalk(t *testing.T) {
	expr := &storage.Node{
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
	}
	pred := &storage.Predicate{
		Root: &storage.Node{
			NodeType: storage.NodeTypeParenExpression,
			Children: []*storage.Node{expr},
		},
	}

	d, err := json.MarshalIndent(pred, " ", " ")
	if err != nil {
		t.Error(err)
	}
	t.Log(string(d))

	t.Log(storage.PredicateToExprString(pred))
}

func TestNodeToExpr(t *testing.T) {

	node := &storage.Node{
		NodeType: storage.NodeTypeComparisonExpression,
		Value:    &storage.Node_Comparison_{Comparison: storage.ComparisonEqual},
		Children: []*storage.Node{
			{NodeType: storage.NodeTypeTagRef, Value: &storage.Node_TagRefValue{TagRefValue: "host"}},
			{NodeType: storage.NodeTypeLiteral, Value: &storage.Node_StringValue{StringValue: "host1"}},
		},
	}

	expr, err := storage.NodeToExpr(node)
	if err != nil {
		t.Error(err)
	}
	t.Log(expr)
}

func TestNodeToExpr2(t *testing.T) {
	pred := &storage.Predicate{
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
	}

	expr, err := storage.NodeToExpr(pred.Root)
	if err != nil {
		t.Error(err)
	}
	t.Log(expr)
}

func TestRewriteExprRemoveFieldKeyAndValue(t *testing.T) {
	pred := &storage.Predicate{
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
		},
	}

	expr, err := storage.NodeToExpr(pred.Root)
	if err != nil {
		t.Error(err)
	}

	expr = storage.RewriteExprRemoveFieldKeyAndValue(expr)
	t.Log(expr)
}
