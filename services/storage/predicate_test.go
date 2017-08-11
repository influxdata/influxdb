package storage_test

import (
	"bytes"
	"strconv"
	"testing"

	"encoding/json"

	"github.com/influxdata/influxdb/services/storage"
)

type visalt1 struct {
	bytes.Buffer
}

func (v *visalt1) Visit(n *storage.Node) storage.NodeVisitor {
	switch n.NodeType {
	case storage.NodeTypeGroupExpression:
		if len(n.Children) > 0 {
			var op string
			if n.GetLogical() == storage.LogicalAnd {
				op = " AND "
			} else {
				op = " OR "
			}
			v.Buffer.WriteString("( ")
			storage.WalkNode(v, n.Children[0])
			for _, e := range n.Children[1:] {
				v.Buffer.WriteString(op)
				storage.WalkNode(v, e)
			}

			v.Buffer.WriteString(" )")
		}

		return nil

	case storage.NodeTypeBooleanExpression:
		storage.WalkNode(v, n.Children[0])
		v.Buffer.WriteByte(' ')
		switch n.GetComparison() {
		case storage.ComparisonEqual:
			v.Buffer.WriteByte('=')
		case storage.ComparisonNotEqual:
			v.Buffer.WriteString("!=")
		case storage.ComparisonStartsWith:
			v.Buffer.WriteString("startsWith")
		case storage.ComparisonRegex:
			v.Buffer.WriteString("=~")
		case storage.ComparisonNotRegex:
			v.Buffer.WriteString("!~")
		}

		v.Buffer.WriteByte(' ')
		storage.WalkNode(v, n.Children[1])
		return nil

	case storage.NodeTypeRef:
		v.Buffer.WriteByte('\'')
		v.Buffer.WriteString(n.GetStringValue())
		v.Buffer.WriteByte('\'')
		return nil

	case storage.NodeTypeLiteral:
		switch val := n.Value.(type) {
		case *storage.Node_StringValue:
			v.Buffer.WriteString(strconv.Quote(val.StringValue))

		case *storage.Node_RegexValue:
			v.Buffer.WriteByte('/')
			v.Buffer.WriteString(val.RegexValue)
			v.Buffer.WriteByte('/')

		case *storage.Node_IntegerValue:
			v.Buffer.WriteString(strconv.FormatInt(val.IntegerValue, 10))

		case *storage.Node_UnsignedValue:
			v.Buffer.WriteString(strconv.FormatUint(val.UnsignedValue, 10))

		case *storage.Node_FloatValue:
			v.Buffer.WriteString(strconv.FormatFloat(val.FloatValue, 'f', 10, 64))

		case *storage.Node_BooleanValue:
			if val.BooleanValue {
				v.Buffer.WriteString("true")
			} else {
				v.Buffer.WriteString("false")
			}
		}

		return nil

	default:
		return v
	}
}

func TestWalk(t *testing.T) {
	var v visalt1

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

	storage.WalkNode(&v, pred.Root)
	t.Log(v.String())
}

func TestNodeToExpr3(t *testing.T) {
	tests := []struct {
		name string
		node *storage.Node
		exp  string
	}{
	// TODO: test cases
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {

		})
	}
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
