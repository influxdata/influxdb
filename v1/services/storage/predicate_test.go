package storage_test

import (
	"testing"

	"github.com/influxdata/influxdb/v2/pkg/testing/assert"
	"github.com/influxdata/influxdb/v2/storage/reads"
	"github.com/influxdata/influxdb/v2/storage/reads/datatypes"
	"github.com/influxdata/influxdb/v2/v1/services/storage"
	"github.com/influxdata/influxql"
)

func TestRewriteExprRemoveFieldKeyAndValue(t *testing.T) {
	node := &datatypes.Node{
		NodeType: datatypes.Node_TypeLogicalExpression,
		Value:    &datatypes.Node_Logical_{Logical: datatypes.Node_LogicalAnd},
		Children: []*datatypes.Node{
			{
				NodeType: datatypes.Node_TypeComparisonExpression,
				Value:    &datatypes.Node_Comparison_{Comparison: datatypes.Node_ComparisonEqual},
				Children: []*datatypes.Node{
					{NodeType: datatypes.Node_TypeTagRef, Value: &datatypes.Node_TagRefValue{TagRefValue: "host"}},
					{NodeType: datatypes.Node_TypeLiteral, Value: &datatypes.Node_StringValue{StringValue: "host1"}},
				},
			},
			{
				NodeType: datatypes.Node_TypeComparisonExpression,
				Value:    &datatypes.Node_Comparison_{Comparison: datatypes.Node_ComparisonRegex},
				Children: []*datatypes.Node{
					{NodeType: datatypes.Node_TypeTagRef, Value: &datatypes.Node_TagRefValue{TagRefValue: "_field"}},
					{NodeType: datatypes.Node_TypeLiteral, Value: &datatypes.Node_RegexValue{RegexValue: "^us-west"}},
				},
			},
			{
				NodeType: datatypes.Node_TypeComparisonExpression,
				Value:    &datatypes.Node_Comparison_{Comparison: datatypes.Node_ComparisonEqual},
				Children: []*datatypes.Node{
					{NodeType: datatypes.Node_TypeFieldRef, Value: &datatypes.Node_FieldRefValue{FieldRefValue: "$"}},
					{NodeType: datatypes.Node_TypeLiteral, Value: &datatypes.Node_FloatValue{FloatValue: 0.5}},
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
