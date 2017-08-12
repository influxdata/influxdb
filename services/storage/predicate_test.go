package storage_test

import (
	"testing"

	"encoding/json"

	"github.com/influxdata/influxdb/services/storage"
)

func TestWalk(t *testing.T) {
	pred := &storage.Predicate{
		Root: &storage.Node{
			NodeType: storage.NodeTypeGroupExpression,
			Value:    &storage.Node_Logical_{Logical: storage.LogicalAnd},
			Children: []*storage.Node{
				{
					NodeType: storage.NodeTypeBooleanExpression,
					Value:    &storage.Node_Comparison_{Comparison: storage.ComparisonEqual},
					Children: []*storage.Node{
						{NodeType: storage.NodeTypeRef, Value: &storage.Node_RefValue{RefValue: "host"}},
						{NodeType: storage.NodeTypeLiteral, Value: &storage.Node_StringValue{StringValue: "host1"}},
					},
				},
				{
					NodeType: storage.NodeTypeBooleanExpression,
					Value:    &storage.Node_Comparison_{Comparison: storage.ComparisonRegex},
					Children: []*storage.Node{
						{NodeType: storage.NodeTypeRef, Value: &storage.Node_RefValue{RefValue: "region"}},
						{NodeType: storage.NodeTypeLiteral, Value: &storage.Node_RegexValue{RegexValue: "^us-west"}},
					},
				},
			},
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
		NodeType: storage.NodeTypeBooleanExpression,
		Value:    &storage.Node_Comparison_{Comparison: storage.ComparisonEqual},
		Children: []*storage.Node{
			{NodeType: storage.NodeTypeRef, Value: &storage.Node_RefValue{RefValue: "host"}},
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
			NodeType: storage.NodeTypeGroupExpression,
			Value:    &storage.Node_Logical_{Logical: storage.LogicalAnd},
			Children: []*storage.Node{
				{
					NodeType: storage.NodeTypeBooleanExpression,
					Value:    &storage.Node_Comparison_{Comparison: storage.ComparisonEqual},
					Children: []*storage.Node{
						{NodeType: storage.NodeTypeRef, Value: &storage.Node_RefValue{RefValue: "host"}},
						{NodeType: storage.NodeTypeLiteral, Value: &storage.Node_StringValue{StringValue: "host1"}},
					},
				},
				{
					NodeType: storage.NodeTypeBooleanExpression,
					Value:    &storage.Node_Comparison_{Comparison: storage.ComparisonRegex},
					Children: []*storage.Node{
						{NodeType: storage.NodeTypeRef, Value: &storage.Node_RefValue{RefValue: "region"}},
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

func TestNodeToExprNoField(t *testing.T) {
	pred := &storage.Predicate{
		Root: &storage.Node{
			NodeType: storage.NodeTypeGroupExpression,
			Value:    &storage.Node_Logical_{Logical: storage.LogicalAnd},
			Children: []*storage.Node{
				{
					NodeType: storage.NodeTypeBooleanExpression,
					Value:    &storage.Node_Comparison_{Comparison: storage.ComparisonEqual},
					Children: []*storage.Node{
						{NodeType: storage.NodeTypeRef, Value: &storage.Node_RefValue{RefValue: "host"}},
						{NodeType: storage.NodeTypeLiteral, Value: &storage.Node_StringValue{StringValue: "host1"}},
					},
				},
				{
					NodeType: storage.NodeTypeBooleanExpression,
					Value:    &storage.Node_Comparison_{Comparison: storage.ComparisonRegex},
					Children: []*storage.Node{
						{NodeType: storage.NodeTypeRef, Value: &storage.Node_RefValue{RefValue: "_field"}},
						{NodeType: storage.NodeTypeLiteral, Value: &storage.Node_RegexValue{RegexValue: "^us-west"}},
					},
				},
			},
		},
	}

	expr, hf, err := storage.NodeToExprNoField(pred.Root)
	if err != nil {
		t.Error(err)
	}
	t.Log(hf)
	t.Log(expr)
}
