package predicate

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/storage/reads/datatypes"
	influxtesting "github.com/influxdata/influxdb/testing"

	"github.com/google/go-cmp/cmp"
)

func TestMarshalDataTypes(t *testing.T) {
	cases := []struct {
		name     string
		node     Node
		dataType *datatypes.Node
	}{
		{
			name: "empty node",
		},
		{
			name: "tag rule",
			node: &TagRuleNode{
				Operator: influxdb.Equal,
				Tag: influxdb.Tag{
					Key:   "k1",
					Value: "v1",
				},
			},
			dataType: &datatypes.Node{
				NodeType: datatypes.NodeTypeComparisonExpression,
				Value:    &datatypes.Node_Comparison_{Comparison: datatypes.ComparisonEqual},
				Children: []*datatypes.Node{
					{
						NodeType: datatypes.NodeTypeTagRef,
						Value:    &datatypes.Node_TagRefValue{TagRefValue: "k1"},
					},
					{
						NodeType: datatypes.NodeTypeLiteral,
						Value: &datatypes.Node_StringValue{
							StringValue: "v1",
						},
					},
				},
			},
		},
		{
			name: "logical",
			node: &LogicalNode{
				Operator: LogicalAnd,
				Children: []Node{
					&TagRuleNode{
						Operator: influxdb.Equal,
						Tag: influxdb.Tag{
							Key:   "k1",
							Value: "v1",
						},
					},
					&TagRuleNode{
						Operator: influxdb.Equal,
						Tag: influxdb.Tag{
							Key:   "k2",
							Value: "v2",
						},
					},
				},
			},
			dataType: &datatypes.Node{
				NodeType: datatypes.NodeTypeLogicalExpression,
				Value: &datatypes.Node_Logical_{
					Logical: datatypes.LogicalAnd,
				},
				Children: []*datatypes.Node{
					{
						NodeType: datatypes.NodeTypeComparisonExpression,
						Value:    &datatypes.Node_Comparison_{Comparison: datatypes.ComparisonEqual},
						Children: []*datatypes.Node{
							{
								NodeType: datatypes.NodeTypeTagRef,
								Value:    &datatypes.Node_TagRefValue{TagRefValue: "k1"},
							},
							{
								NodeType: datatypes.NodeTypeLiteral,
								Value: &datatypes.Node_StringValue{
									StringValue: "v1",
								},
							},
						},
					},
					{
						NodeType: datatypes.NodeTypeComparisonExpression,
						Value:    &datatypes.Node_Comparison_{Comparison: datatypes.ComparisonEqual},
						Children: []*datatypes.Node{
							{
								NodeType: datatypes.NodeTypeTagRef,
								Value:    &datatypes.Node_TagRefValue{TagRefValue: "k2"},
							},
							{
								NodeType: datatypes.NodeTypeLiteral,
								Value: &datatypes.Node_StringValue{
									StringValue: "v2",
								},
							},
						},
					},
				},
			},
		},
		{
			name: "conplex logical",
			node: &LogicalNode{
				Operator: LogicalAnd,
				Children: []Node{
					&LogicalNode{
						Operator: LogicalAnd,
						Children: []Node{
							&TagRuleNode{
								Operator: influxdb.Equal,
								Tag: influxdb.Tag{
									Key:   "k3",
									Value: "v3",
								},
							},
							&TagRuleNode{
								Operator: influxdb.Equal,
								Tag: influxdb.Tag{
									Key:   "k4",
									Value: "v4",
								},
							},
						},
					},
					&TagRuleNode{
						Operator: influxdb.Equal,
						Tag: influxdb.Tag{
							Key:   "k2",
							Value: "v2",
						},
					},
				},
			},
			dataType: &datatypes.Node{
				NodeType: datatypes.NodeTypeLogicalExpression,
				Value: &datatypes.Node_Logical_{
					Logical: datatypes.LogicalAnd,
				},
				Children: []*datatypes.Node{
					{
						NodeType: datatypes.NodeTypeLogicalExpression,
						Value: &datatypes.Node_Logical_{
							Logical: datatypes.LogicalAnd,
						},
						Children: []*datatypes.Node{
							{
								NodeType: datatypes.NodeTypeComparisonExpression,
								Value:    &datatypes.Node_Comparison_{Comparison: datatypes.ComparisonEqual},
								Children: []*datatypes.Node{
									{
										NodeType: datatypes.NodeTypeTagRef,
										Value:    &datatypes.Node_TagRefValue{TagRefValue: "k3"},
									},
									{
										NodeType: datatypes.NodeTypeLiteral,
										Value: &datatypes.Node_StringValue{
											StringValue: "v3",
										},
									},
								},
							},
							{
								NodeType: datatypes.NodeTypeComparisonExpression,
								Value:    &datatypes.Node_Comparison_{Comparison: datatypes.ComparisonEqual},
								Children: []*datatypes.Node{
									{
										NodeType: datatypes.NodeTypeTagRef,
										Value:    &datatypes.Node_TagRefValue{TagRefValue: "k4"},
									},
									{
										NodeType: datatypes.NodeTypeLiteral,
										Value: &datatypes.Node_StringValue{
											StringValue: "v4",
										},
									},
								},
							},
						},
					},
					{
						NodeType: datatypes.NodeTypeComparisonExpression,
						Value:    &datatypes.Node_Comparison_{Comparison: datatypes.ComparisonEqual},
						Children: []*datatypes.Node{
							{
								NodeType: datatypes.NodeTypeTagRef,
								Value:    &datatypes.Node_TagRefValue{TagRefValue: "k2"},
							},
							{
								NodeType: datatypes.NodeTypeLiteral,
								Value: &datatypes.Node_StringValue{
									StringValue: "v2",
								},
							},
						},
					},
				},
			},
		},
	}
	for _, c := range cases {
		b, err := json.Marshal(c.node)
		if err != nil {
			t.Fatalf("%s marshal failed, err: %s", c.name, err.Error())
		}
		node, err := UnmarshalJSON(b)
		if err != nil {
			t.Fatalf("%s unmarshal failed, err: %s", c.name, err.Error())
		}
		if diff := cmp.Diff(c.node, node); diff != "" {
			t.Fatalf("%s failed nodes are different, diff: %s", c.name, diff)
		}
		if node != nil {
			dataType, err := node.ToDataType()
			if err != nil {
				t.Fatalf("%s ToDataType failed, err: %s", c.name, err.Error())
			}
			if diff := cmp.Diff(dataType, c.dataType); diff != "" {
				t.Fatalf("%s failed nodes are different, diff: %s", c.name, diff)
			}
		}

		_, err = New(node)
		if err != nil {
			t.Fatalf("%s convert to predicate failed, err: %s", c.name, err.Error())
		}
	}
}

func TestError(t *testing.T) {
	cases := []struct {
		json         []byte
		unmarshalErr error
		predErr      error
	}{
		{
			json: []byte(""),
		},
		{
			json: []byte("{}"),
		},
		{
			json: []byte("[]"),
			unmarshalErr: &influxdb.Error{
				Code: influxdb.EInvalid,
				Msg:  "unable to detect the node type from json",
			},
		},
		{
			json: []byte(`{"nodeType": "badType"}`),
			unmarshalErr: &influxdb.Error{
				Code: influxdb.EInvalid,
				Msg:  "invalid node type badType",
			},
		},
		{
			json: []byte(`{"nodeType": "logical"}`),
			predErr: &influxdb.Error{
				Code: influxdb.EInvalid,
				Msg:  `the logical operator "" is invalid`,
			},
		},
		{
			json: []byte(`{
				"nodeType": "logical",
				"operator":"and",
				"children": {}
			}`),
			unmarshalErr: &influxdb.Error{
				Code: influxdb.EInvalid,
				Err:  fmt.Errorf("json: cannot unmarshal object into Go struct field logicalNodeDecode.children of"),
			},
		},
		{
			json: []byte(`{
				"nodeType": "logical",
				"operator":"and",
				"children": []
			}`),
			predErr: &influxdb.Error{
				Code: influxdb.EInvalid,
				Err:  fmt.Errorf("invalid number of children for logical expression: 0"),
			},
		},
		{
			json: []byte(`{
				"nodeType": "logical",
				"operator":"and",
				"children": [
					{"nodeType":"bad"}
				]
			}`),
			unmarshalErr: &influxdb.Error{
				Code: influxdb.EInvalid,
				Msg:  "Err in Child 0, err: invalid node type bad",
			},
		},
		{
			json: []byte(`{
				"nodeType": "logical",
				"operator":"and",
				"children": [
					{"nodeType":"tagRule"}
				]
			}`),
			predErr: &influxdb.Error{
				Code: influxdb.EInvalid,
				Msg:  "Err in Child 0, err: Unsupported operator: ",
			},
		},
		{
			json: []byte(`
			{
				"nodeType": "tagRule",
				"operator":"notequal",
				"key":"k1",
				"value":"v1"
			}`),
			predErr: &influxdb.Error{
				Code: influxdb.EInvalid,
				Msg:  "Operator notequal is not supported for delete predicate yet",
			},
		},
		{
			json: []byte(`
			{
				"nodeType": "tagRule",
				"operator":"notequalregex",
				"key":"k1",
				"value":"v1"
			}`),
			predErr: &influxdb.Error{
				Code: influxdb.EInvalid,
				Msg:  "Operator notequalregex is not supported for delete predicate yet",
			},
		},
		{
			json: []byte(`
			{
				"nodeType": "tagRule",
				"operator":"greater",
				"key":"k1",
				"value":"v1"
			}`),
			predErr: &influxdb.Error{
				Code: influxdb.EInvalid,
				Msg:  "Unsupported operator: greater",
			},
		},
	}
	for _, c := range cases {
		n, err := UnmarshalJSON(c.json)
		influxtesting.ErrorsEqual(t, err, c.unmarshalErr)
		if err != nil {
			continue
		}
		_, err = New(n)
		influxtesting.ErrorsEqual(t, err, c.predErr)
	}
}
