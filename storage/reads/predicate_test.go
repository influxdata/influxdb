package reads_test

import (
	"testing"

	"github.com/influxdata/influxdb/storage/reads"
	"github.com/influxdata/influxdb/storage/reads/datatypes"
)

func TestPredicateToExprString(t *testing.T) {
	cases := []struct {
		n string
		r *datatypes.Predicate
		e string
	}{
		{
			n: "returns [none] for nil",
			r: nil,
			e: "[none]",
		},
		{
			n: "logical AND",
			r: &datatypes.Predicate{
				Root: &datatypes.Node{
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
								{NodeType: datatypes.Node_TypeTagRef, Value: &datatypes.Node_TagRefValue{TagRefValue: "region"}},
								{NodeType: datatypes.Node_TypeLiteral, Value: &datatypes.Node_RegexValue{RegexValue: "^us-west"}},
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
			if got, wanted := reads.PredicateToExprString(tc.r), tc.e; got != wanted {
				t.Fatal("got:", got, "wanted:", wanted)
			}
		})
	}
}
