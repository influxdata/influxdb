package tsm1

import (
	"fmt"
	"testing"

	"github.com/influxdata/influxdb/storage/reads/datatypes"
)

func TestPredicate(t *testing.T) {
	cases := []struct {
		Name      string
		Predicate *datatypes.Predicate
		Key       string
		Matches   bool
	}{
		{
			Name: "Basic Matching",
			Predicate: predicate(
				comparisonNode(datatypes.ComparisonEqual, tagNode("tag3"), stringNode("val3")),
			),
			Key:     "bucketorg,tag3=val3",
			Matches: true,
		},

		{
			Name: "Basic Unmatching",
			Predicate: predicate(
				comparisonNode(datatypes.ComparisonEqual, tagNode("tag3"), stringNode("val3")),
			),
			Key:     "bucketorg,tag3=val2",
			Matches: false,
		},

		{
			Name: "Compound Logical Matching",
			Predicate: predicate(
				orNode(
					andNode(
						comparisonNode(datatypes.ComparisonEqual, tagNode("foo"), stringNode("bar")),
						comparisonNode(datatypes.ComparisonEqual, tagNode("baz"), stringNode("no")),
					),
					comparisonNode(datatypes.ComparisonEqual, tagNode("tag3"), stringNode("val3")),
				),
			),
			Key:     "bucketorg,foo=bar,baz=bif,tag3=val3",
			Matches: true,
		},

		{
			Name: "Compound Logical Unmatching",
			Predicate: predicate(
				orNode(
					andNode(
						comparisonNode(datatypes.ComparisonEqual, tagNode("foo"), stringNode("bar")),
						comparisonNode(datatypes.ComparisonEqual, tagNode("baz"), stringNode("no")),
					),
					comparisonNode(datatypes.ComparisonEqual, tagNode("tag3"), stringNode("val3")),
				),
			),
			Key:     "bucketorg,foo=bar,baz=bif,tag3=val2",
			Matches: false,
		},
	}

	for _, test := range cases {
		t.Run(test.Name, func(t *testing.T) {
			pred, err := NewProtobufPredicate(test.Predicate)
			if err != nil {
				t.Fatal("compile failure:", err)
			}

			if got, exp := pred.Matches([]byte(test.Key)), test.Matches; got != exp {
				t.Fatal("match failure:", "got", got, "!=", "exp", exp)
			}
		})
	}
}

func BenchmarkPredicate(b *testing.B) {
	run := func(b *testing.B, predicate *datatypes.Predicate) {
		pred, err := NewProtobufPredicate(predicate)
		if err != nil {
			b.Fatal(err)
		}

		series := []byte("bucketorg,")
		for i := 0; i < 10; i++ {
			series = append(series, fmt.Sprintf("tag%d=val%d,", i, i)...)
		}
		series = series[:len(series)-1]

		b.SetBytes(int64(len(series)))
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			pred.Matches(series)
		}
	}

	b.Run("Basic", func(b *testing.B) {
		run(b, predicate(
			comparisonNode(datatypes.ComparisonEqual, tagNode("tag5"), stringNode("val5")),
		))
	})

	b.Run("Compound", func(b *testing.B) {
		run(b, predicate(
			orNode(
				andNode(
					comparisonNode(datatypes.ComparisonEqual, tagNode("tag0"), stringNode("val0")),
					comparisonNode(datatypes.ComparisonEqual, tagNode("tag6"), stringNode("val5")),
				),
				comparisonNode(datatypes.ComparisonEqual, tagNode("tag5"), stringNode("val5")),
			),
		))
	})
}

//
// Helpers to create predicate protobufs
//

func tagNode(s string) *datatypes.Node {
	return &datatypes.Node{
		NodeType: datatypes.NodeTypeTagRef,
		Value:    &datatypes.Node_TagRefValue{TagRefValue: s},
	}
}

func stringNode(s string) *datatypes.Node {
	return &datatypes.Node{
		NodeType: datatypes.NodeTypeLiteral,
		Value:    &datatypes.Node_StringValue{StringValue: s},
	}
}

func comparisonNode(comp datatypes.Node_Comparison, left, right *datatypes.Node) *datatypes.Node {
	return &datatypes.Node{
		NodeType: datatypes.NodeTypeComparisonExpression,
		Value:    &datatypes.Node_Comparison_{Comparison: comp},
		Children: []*datatypes.Node{left, right},
	}
}

func andNode(left, right *datatypes.Node) *datatypes.Node {
	return &datatypes.Node{
		NodeType: datatypes.NodeTypeLogicalExpression,
		Value:    &datatypes.Node_Logical_{Logical: datatypes.LogicalAnd},
		Children: []*datatypes.Node{left, right},
	}
}

func orNode(left, right *datatypes.Node) *datatypes.Node {
	return &datatypes.Node{
		NodeType: datatypes.NodeTypeLogicalExpression,
		Value:    &datatypes.Node_Logical_{Logical: datatypes.LogicalOr},
		Children: []*datatypes.Node{left, right},
	}
}

func predicate(root *datatypes.Node) *datatypes.Predicate {
	return &datatypes.Predicate{Root: root}
}
