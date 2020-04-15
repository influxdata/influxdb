package predicate

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/storage/reads/datatypes"
	influxtesting "github.com/influxdata/influxdb/v2/testing"
)

func TestDataTypeConversion(t *testing.T) {
	cases := []struct {
		name     string
		node     Node
		err      error
		dataType *datatypes.Node
	}{
		{
			name: "empty node",
		},
		{
			name: "equal tag rule",
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
			name: "not equal tag rule",
			node: &TagRuleNode{
				Operator: influxdb.NotEqual,
				Tag: influxdb.Tag{
					Key:   "k1",
					Value: "v1",
				},
			},
			dataType: &datatypes.Node{
				NodeType: datatypes.NodeTypeComparisonExpression,
				Value:    &datatypes.Node_Comparison_{Comparison: datatypes.ComparisonNotEqual},
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
			name: "measurement equal tag rule",
			node: &TagRuleNode{
				Operator: influxdb.Equal,
				Tag: influxdb.Tag{
					Key:   "_measurement",
					Value: "cpu",
				},
			},
			dataType: &datatypes.Node{
				NodeType: datatypes.NodeTypeComparisonExpression,
				Value:    &datatypes.Node_Comparison_{Comparison: datatypes.ComparisonEqual},
				Children: []*datatypes.Node{
					{
						NodeType: datatypes.NodeTypeTagRef,
						Value:    &datatypes.Node_TagRefValue{TagRefValue: models.MeasurementTagKey},
					},
					{
						NodeType: datatypes.NodeTypeLiteral,
						Value: &datatypes.Node_StringValue{
							StringValue: "cpu",
						},
					},
				},
			},
		},
		{
			name: "measurement not equal tag rule",
			node: &TagRuleNode{
				Operator: influxdb.NotEqual,
				Tag: influxdb.Tag{
					Key:   "_measurement",
					Value: "cpu",
				},
			},
			dataType: &datatypes.Node{
				NodeType: datatypes.NodeTypeComparisonExpression,
				Value:    &datatypes.Node_Comparison_{Comparison: datatypes.ComparisonNotEqual},
				Children: []*datatypes.Node{
					{
						NodeType: datatypes.NodeTypeTagRef,
						Value:    &datatypes.Node_TagRefValue{TagRefValue: models.MeasurementTagKey},
					},
					{
						NodeType: datatypes.NodeTypeLiteral,
						Value: &datatypes.Node_StringValue{
							StringValue: "cpu",
						},
					},
				},
			},
		},
		{
			name: "equal field tag rule",
			node: &TagRuleNode{
				Operator: influxdb.Equal,
				Tag: influxdb.Tag{
					Key:   "_field",
					Value: "cpu",
				},
			},
			dataType: &datatypes.Node{
				NodeType: datatypes.NodeTypeComparisonExpression,
				Value:    &datatypes.Node_Comparison_{Comparison: datatypes.ComparisonEqual},
				Children: []*datatypes.Node{
					{
						NodeType: datatypes.NodeTypeTagRef,
						Value:    &datatypes.Node_TagRefValue{TagRefValue: models.FieldKeyTagKey},
					},
					{
						NodeType: datatypes.NodeTypeLiteral,
						Value: &datatypes.Node_StringValue{
							StringValue: "cpu",
						},
					},
				},
			},
		},
		{
			name: "not equal field tag rule",
			node: &TagRuleNode{
				Operator: influxdb.NotEqual,
				Tag: influxdb.Tag{
					Key:   "_field",
					Value: "cpu",
				},
			},
			dataType: &datatypes.Node{
				NodeType: datatypes.NodeTypeComparisonExpression,
				Value:    &datatypes.Node_Comparison_{Comparison: datatypes.ComparisonNotEqual},
				Children: []*datatypes.Node{
					{
						NodeType: datatypes.NodeTypeTagRef,
						Value:    &datatypes.Node_TagRefValue{TagRefValue: models.FieldKeyTagKey},
					},
					{
						NodeType: datatypes.NodeTypeLiteral,
						Value: &datatypes.Node_StringValue{
							StringValue: "cpu",
						},
					},
				},
			},
		},
		{
			name: "logical",
			node: &LogicalNode{
				Operator: LogicalAnd,
				Children: [2]Node{
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
				Children: [2]Node{
					&LogicalNode{
						Operator: LogicalAnd,
						Children: [2]Node{
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
		if c.node != nil {
			dataType, err := c.node.ToDataType()
			influxtesting.ErrorsEqual(t, err, c.err)
			if c.err != nil {
				continue
			}
			if diff := cmp.Diff(dataType, c.dataType); diff != "" {
				t.Fatalf("%s failed nodes are different, diff: %s", c.name, diff)
			}
		}

		if _, err := New(c.node); err != nil {
			t.Fatalf("%s convert to predicate failed, err: %s", c.name, err.Error())
		}
	}
}
