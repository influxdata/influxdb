package predicate

import (
	"fmt"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/storage/reads/datatypes"
)

// TagRuleNode is a node type of a single tag rule.
type TagRuleNode influxdb.TagRule

var specialKey = map[string]string{
	"_measurement": models.MeasurementTagKey,
	"_field":       models.FieldKeyTagKey,
}

// NodeTypeLiteral convert a TagRuleNode to a nodeTypeLiteral.
func NodeTypeLiteral(tr TagRuleNode) *datatypes.Node {
	switch tr.Operator {
	case influxdb.RegexEqual:
		fallthrough
	case influxdb.NotRegexEqual:
		return &datatypes.Node{
			NodeType: datatypes.Node_TypeLiteral,
			Value: &datatypes.Node_RegexValue{
				RegexValue: tr.Value,
			},
		}
	default:
		return &datatypes.Node{
			NodeType: datatypes.Node_TypeLiteral,
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
		return datatypes.Node_ComparisonEqual, nil
	case influxdb.NotEqual:
		return datatypes.Node_ComparisonNotEqual, nil
	case influxdb.RegexEqual:
		fallthrough
	case influxdb.NotRegexEqual:
		return 0, &errors.Error{
			Code: errors.EInvalid,
			Msg:  fmt.Sprintf("Operator %s is not supported for delete predicate yet", op),
		}
	default:
		return 0, &errors.Error{
			Code: errors.EInvalid,
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
	if special, ok := specialKey[n.Key]; ok {
		n.Key = special
	}
	return &datatypes.Node{
		NodeType: datatypes.Node_TypeComparisonExpression,
		Value:    &datatypes.Node_Comparison_{Comparison: compare},
		Children: []*datatypes.Node{
			{
				NodeType: datatypes.Node_TypeTagRef,
				Value:    &datatypes.Node_TagRefValue{TagRefValue: n.Key},
			},
			NodeTypeLiteral(n),
		},
	}, nil
}
