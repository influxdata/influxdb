package predicate

import (
	"encoding/json"
	"fmt"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/storage/reads/datatypes"
)

// TagRuleNode is a node type of a single tag rule.
type TagRuleNode influxdb.TagRule

// NodeType returns value "logical".
func (n TagRuleNode) NodeType() string {
	return tagRuleNodeType
}

type tagRuleNodeAlias TagRuleNode

// MarshalJSON implement json.Marshaler interface.
func (n TagRuleNode) MarshalJSON() ([]byte, error) {
	return json.Marshal(
		struct {
			tagRuleNodeAlias
			NodeType string `json:"nodeType"`
		}{
			tagRuleNodeAlias: tagRuleNodeAlias(n),
			NodeType:         n.NodeType(),
		})
}

// NodeTypeLiteral convert a TagRuleNode to a nodeTypeLiteral.
func NodeTypeLiteral(tr TagRuleNode) *datatypes.Node {
	switch tr.Operator {
	case influxdb.RegexEqual:
		fallthrough
	case influxdb.NotRegexEqual:
		return &datatypes.Node{
			NodeType: datatypes.NodeTypeLiteral,
			Value: &datatypes.Node_RegexValue{
				RegexValue: tr.Value,
			},
		}
	default:
		return &datatypes.Node{
			NodeType: datatypes.NodeTypeLiteral,
			Value: &datatypes.Node_StringValue{
				StringValue: tr.Value,
			},
		}
	}
}

// NodeComparison convert influxdb.Operator to Node_Comparison.
func NodeComparison(op influxdb.Operator) (datatypes.Node_Comparison, error) {
	switch op {
	case influxdb.Equal:
		return datatypes.ComparisonEqual, nil
	case influxdb.NotEqual:
		return datatypes.ComparisonNotEqual, nil
	case influxdb.RegexEqual:
		return datatypes.ComparisonRegex, nil
	case influxdb.NotRegexEqual:
		return datatypes.ComparisonNotRegex, nil
	default:
		return 0, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  fmt.Sprintf("Unsupported operator: %s", op),
		}
	}

}

// ToDataType convert a TagRuleNode to datatypes.Node.
func (n TagRuleNode) ToDataType() (*datatypes.Node, error) {
	compare, err := NodeComparison(n.Operator)
	if err != nil {
		return nil, err
	}
	return &datatypes.Node{
		NodeType: datatypes.NodeTypeComparisonExpression,
		Value:    &datatypes.Node_Comparison_{Comparison: compare},
		Children: []*datatypes.Node{
			{
				NodeType: datatypes.NodeTypeTagRef,
				Value:    &datatypes.Node_TagRefValue{TagRefValue: n.Key},
			},
			NodeTypeLiteral(n),
		},
	}, nil
}
