package tsm1

import (
	"fmt"
	"testing"

	"github.com/influxdata/influxdb/v2/storage/reads/datatypes"
)

func TestPredicatePopTagEscape(t *testing.T) {
	cases := []struct {
		Key   string
		Tag   string
		Value string
		Rest  string
	}{
		{Key: "", Tag: "", Value: "", Rest: ""},
		{Key: "invalid", Tag: "", Value: "", Rest: ""},
		{Key: "region=west,server=b,foo=bar", Tag: "region", Value: "west", Rest: "server=b,foo=bar"},
		{Key: "region=west", Tag: "region", Value: "west", Rest: ""},
		{Key: `re\=gion=west,server=a`, Tag: `re=gion`, Value: "west", Rest: "server=a"},
		{Key: `region=w\,est,server=a`, Tag: `region`, Value: "w,est", Rest: "server=a"},
		{Key: `hi\ yo\ =w\,est,server=a`, Tag: `hi yo `, Value: "w,est", Rest: "server=a"},
		{Key: `\ e\ \=o=world,server=a`, Tag: ` e =o`, Value: "world", Rest: "server=a"},
	}

	for _, c := range cases {
		tag, value, rest := predicatePopTagEscape([]byte(c.Key))
		if string(tag) != c.Tag {
			t.Fatalf("got returned tag %q expected %q", tag, c.Tag)
		} else if string(value) != c.Value {
			t.Fatalf("got returned value %q expected %q", value, c.Value)
		} else if string(rest) != c.Rest {
			t.Fatalf("got returned remainder %q expected %q", rest, c.Rest)
		}
	}
}

func TestPredicate_Matches(t *testing.T) {
	cases := []struct {
		Name      string
		Predicate *datatypes.Predicate
		Key       string
		Matches   bool
	}{
		{
			Name: "Basic Matching",
			Predicate: predicate(
				comparisonNode(datatypes.Node_ComparisonEqual, tagNode("tag3"), stringNode("val3"))),
			Key:     "bucketorg,tag3=val3",
			Matches: true,
		},

		{
			Name: "Basic Unmatching",
			Predicate: predicate(
				comparisonNode(datatypes.Node_ComparisonEqual, tagNode("tag3"), stringNode("val3"))),
			Key:     "bucketorg,tag3=val2",
			Matches: false,
		},

		{
			Name: "Compound Logical Matching",
			Predicate: predicate(
				orNode(
					andNode(
						comparisonNode(datatypes.Node_ComparisonEqual, tagNode("foo"), stringNode("bar")),
						comparisonNode(datatypes.Node_ComparisonEqual, tagNode("baz"), stringNode("no"))),
					comparisonNode(datatypes.Node_ComparisonEqual, tagNode("tag3"), stringNode("val3")))),
			Key:     "bucketorg,foo=bar,baz=bif,tag3=val3",
			Matches: true,
		},

		{
			Name: "Compound Logical Unmatching",
			Predicate: predicate(
				orNode(
					andNode(
						comparisonNode(datatypes.Node_ComparisonEqual, tagNode("foo"), stringNode("bar")),
						comparisonNode(datatypes.Node_ComparisonEqual, tagNode("baz"), stringNode("no"))),
					comparisonNode(datatypes.Node_ComparisonEqual, tagNode("tag3"), stringNode("val3")))),
			Key:     "bucketorg,foo=bar,baz=bif,tag3=val2",
			Matches: false,
		},

		{
			Name: "Logical Or Short Circuit",
			Predicate: predicate(
				orNode(
					comparisonNode(datatypes.Node_ComparisonEqual, tagNode("foo"), stringNode("bar")),
					comparisonNode(datatypes.Node_ComparisonEqual, tagNode("baz"), stringNode("no")))),
			Key:     "bucketorg,baz=bif,foo=bar,tag3=val3",
			Matches: true,
		},

		{
			Name: "Logical And Short Circuit",
			Predicate: predicate(
				andNode(
					comparisonNode(datatypes.Node_ComparisonEqual, tagNode("foo"), stringNode("no")),
					comparisonNode(datatypes.Node_ComparisonEqual, tagNode("baz"), stringNode("bif")))),
			Key:     "bucketorg,baz=bif,foo=bar,tag3=val3",
			Matches: false,
		},

		{
			Name: "Logical And Matching",
			Predicate: predicate(
				andNode(
					comparisonNode(datatypes.Node_ComparisonEqual, tagNode("foo"), stringNode("bar")),
					comparisonNode(datatypes.Node_ComparisonEqual, tagNode("baz"), stringNode("bif")))),
			Key:     "bucketorg,baz=bif,foo=bar,tag3=val3",
			Matches: true,
		},

		{
			Name: "Logical And Matching Reduce (Simplify)",
			Predicate: predicate(
				andNode(
					comparisonNode(datatypes.Node_ComparisonEqual, tagNode("foo"), stringNode("bar")),
					comparisonNode(datatypes.Node_ComparisonNotEqual, tagNode("foo"), stringNode("bif")))),
			Key:     "bucketorg,baz=bif,foo=bar,tag3=val3",
			Matches: true,
		},

		{
			Name: "Regex Matching",
			Predicate: predicate(
				comparisonNode(datatypes.Node_ComparisonRegex, tagNode("tag3"), regexNode("...3"))),
			Key:     "bucketorg,tag3=val3",
			Matches: true,
		},

		{
			Name: "NotRegex Matching",
			Predicate: predicate(
				comparisonNode(datatypes.Node_ComparisonNotRegex, tagNode("tag3"), regexNode("...4"))),
			Key:     "bucketorg,tag3=val3",
			Matches: true,
		},

		{
			Name: "Regex Unmatching",
			Predicate: predicate(
				comparisonNode(datatypes.Node_ComparisonRegex, tagNode("tag3"), regexNode("...4"))),
			Key:     "bucketorg,tag3=val3",
			Matches: false,
		},

		{
			Name: "NotRegex Unmatching",
			Predicate: predicate(
				comparisonNode(datatypes.Node_ComparisonNotRegex, tagNode("tag3"), regexNode("...3"))),
			Key:     "bucketorg,tag3=val3",
			Matches: false,
		},

		{
			Name: "Basic Matching Reversed",
			Predicate: predicate(
				comparisonNode(datatypes.Node_ComparisonEqual, stringNode("val3"), tagNode("tag3"))),
			Key:     "bucketorg,tag2=val2,tag3=val3",
			Matches: true,
		},

		{
			Name: "Tag Matching Tag",
			Predicate: predicate(
				comparisonNode(datatypes.Node_ComparisonEqual, tagNode("tag4"), tagNode("tag3"))),
			Key:     "bucketorg,tag3=val3,tag4=val3",
			Matches: true,
		},

		{
			Name: "No Tag",
			Predicate: predicate(
				comparisonNode(datatypes.Node_ComparisonEqual, tagNode("tag4"), stringNode("val4"))),
			Key:     "bucketorg,tag3=val3",
			Matches: false,
		},

		{
			Name: "Not Equal",
			Predicate: predicate(
				comparisonNode(datatypes.Node_ComparisonNotEqual, tagNode("tag3"), stringNode("val4"))),
			Key:     "bucketorg,tag3=val3",
			Matches: true,
		},

		{
			Name: "Starts With",
			Predicate: predicate(
				comparisonNode(datatypes.Node_ComparisonStartsWith, tagNode("tag3"), stringNode("va"))),
			Key:     "bucketorg,tag3=val3",
			Matches: true,
		},

		{
			Name: "Less",
			Predicate: predicate(
				comparisonNode(datatypes.Node_ComparisonLess, tagNode("tag3"), stringNode("val4"))),
			Key:     "bucketorg,tag3=val3",
			Matches: true,
		},

		{
			Name: "Less Equal",
			Predicate: predicate(
				comparisonNode(datatypes.Node_ComparisonLessEqual, tagNode("tag3"), stringNode("val4"))),
			Key:     "bucketorg,tag3=val3",
			Matches: true,
		},

		{
			Name: "Greater",
			Predicate: predicate(
				comparisonNode(datatypes.Node_ComparisonGreater, tagNode("tag3"), stringNode("u"))),
			Key:     "bucketorg,tag3=val3",
			Matches: true,
		},

		{
			Name: "Greater Equal;",
			Predicate: predicate(
				comparisonNode(datatypes.Node_ComparisonGreaterEqual, tagNode("tag3"), stringNode("u"))),
			Key:     "bucketorg,tag3=val3",
			Matches: true,
		},

		{
			Name: "Escaping Matching",
			Predicate: predicate(
				comparisonNode(datatypes.Node_ComparisonEqual, tagNode("tag3"), stringNode("val3"))),
			Key:     `bucketorg,tag1=\,foo,tag2=\ bar,tag2\=more=val2\,\ \=hello,tag3=val3`,
			Matches: true,
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

			// Clone and try again.
			pred = pred.Clone()
			if got, exp := pred.Matches([]byte(test.Key)), test.Matches; got != exp {
				t.Fatal("cloned match failure:", "got", got, "!=", "exp", exp)
			}
		})
	}
}

func TestPredicate_Unmarshal(t *testing.T) {
	protoPred := predicate(
		orNode(
			andNode(
				comparisonNode(datatypes.Node_ComparisonEqual, tagNode("foo"), stringNode("bar")),
				comparisonNode(datatypes.Node_ComparisonEqual, tagNode("baz"), stringNode("no"))),
			comparisonNode(datatypes.Node_ComparisonEqual, tagNode("tag3"), stringNode("val3"))))

	pred1, err := NewProtobufPredicate(protoPred)
	if err != nil {
		t.Fatal(err)
	}

	predData, err := pred1.Marshal()
	if err != nil {
		t.Fatal(err)
	}

	_, err = UnmarshalPredicate(predData)
	if err != nil {
		t.Fatal(err)
	}

	t.Skip("TODO(dstrand1): Fix cmp for predicateMatcher. See in IDPE: https://github.com/influxdata/idpe/blob/7c52ef7c9bc387905f2864c8730c7366f07f8a1e/storage/tsdb/tsm1/predicate_test.go#L285")

	//if !cmp.Equal(pred1, pred2, cmputil.IgnoreProtobufUnexported()) {
	//	t.Fatal("mismatch on unmarshal")
	//}
}

func TestPredicate_Unmarshal_InvalidTag(t *testing.T) {
	_, err := UnmarshalPredicate([]byte("\xff"))
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestPredicate_Unmarshal_InvalidProtobuf(t *testing.T) {
	_, err := UnmarshalPredicate([]byte("\x00\xff"))
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestPredicate_Unmarshal_Empty(t *testing.T) {
	pred, err := UnmarshalPredicate(nil)
	if err != nil {
		t.Fatal(err)
	} else if pred != nil {
		t.Fatal("expected no predicate")
	}
}

func TestPredicate_Invalid_Protobuf(t *testing.T) {
	cases := []struct {
		Name      string
		Predicate *datatypes.Predicate
	}{
		{
			Name: "Invalid Comparison Num Children",
			Predicate: predicate(&datatypes.Node{
				NodeType: datatypes.Node_TypeComparisonExpression,
				Value:    &datatypes.Node_Comparison_{Comparison: datatypes.Node_ComparisonEqual},
				Children: []*datatypes.Node{{}, {}, {}},
			}),
		},

		{
			Name: "Mismatching Left Tag Type",
			Predicate: predicate(
				comparisonNode(datatypes.Node_ComparisonEqual, &datatypes.Node{
					NodeType: datatypes.Node_TypeTagRef,
					Value:    &datatypes.Node_IntegerValue{IntegerValue: 2},
				}, tagNode("tag"))),
		},

		{
			Name: "Mismatching Left Literal Type",
			Predicate: predicate(
				comparisonNode(datatypes.Node_ComparisonEqual, &datatypes.Node{
					NodeType: datatypes.Node_TypeLiteral,
					Value:    &datatypes.Node_IntegerValue{IntegerValue: 2},
				}, tagNode("tag"))),
		},

		{
			Name: "Invalid Left Node Type",
			Predicate: predicate(
				comparisonNode(datatypes.Node_ComparisonEqual, &datatypes.Node{
					NodeType: datatypes.Node_TypeComparisonExpression,
					Value:    &datatypes.Node_Comparison_{Comparison: datatypes.Node_ComparisonEqual},
				}, tagNode("tag"))),
		},

		{
			Name: "Mismatching Right Tag Type",
			Predicate: predicate(
				comparisonNode(datatypes.Node_ComparisonEqual, tagNode("tag"), &datatypes.Node{
					NodeType: datatypes.Node_TypeTagRef,
					Value:    &datatypes.Node_IntegerValue{IntegerValue: 2},
				})),
		},

		{
			Name: "Invalid Regex",
			Predicate: predicate(
				comparisonNode(datatypes.Node_ComparisonRegex, tagNode("tag3"), regexNode("("))),
		},

		{
			Name: "Mismatching Right Literal Type",
			Predicate: predicate(
				comparisonNode(datatypes.Node_ComparisonEqual, tagNode("tag"), &datatypes.Node{
					NodeType: datatypes.Node_TypeLiteral,
					Value:    &datatypes.Node_IntegerValue{IntegerValue: 2},
				})),
		},

		{
			Name: "Invalid Right Node Type",
			Predicate: predicate(
				comparisonNode(datatypes.Node_ComparisonEqual, tagNode("tag"), &datatypes.Node{
					NodeType: datatypes.Node_TypeComparisonExpression,
					Value:    &datatypes.Node_Comparison_{Comparison: datatypes.Node_ComparisonEqual},
				})),
		},

		{
			Name: "Invalid Comparison Without Regex",
			Predicate: predicate(
				comparisonNode(datatypes.Node_ComparisonRegex, tagNode("tag3"), stringNode("val3"))),
		},

		{
			Name: "Invalid Comparison With Regex",
			Predicate: predicate(
				comparisonNode(datatypes.Node_ComparisonEqual, tagNode("tag3"), regexNode("."))),
		},

		{
			Name: "Invalid Logical Operation Children",
			Predicate: predicate(&datatypes.Node{
				NodeType: datatypes.Node_TypeLogicalExpression,
				Value:    &datatypes.Node_Logical_{Logical: datatypes.Node_LogicalAnd},
				Children: []*datatypes.Node{{}, {}, {}},
			}),
		},

		{
			Name: "Invalid Left Logical Expression",
			Predicate: predicate(
				andNode(
					tagNode("tag"),
					comparisonNode(datatypes.Node_ComparisonEqual, tagNode("tag3"), stringNode("val3")),
				)),
		},

		{
			Name: "Invalid Right Logical Expression",
			Predicate: predicate(
				andNode(
					comparisonNode(datatypes.Node_ComparisonEqual, tagNode("tag3"), stringNode("val3")),
					tagNode("tag"),
				)),
		},

		{
			Name: "Invalid Logical Value",
			Predicate: predicate(&datatypes.Node{
				NodeType: datatypes.Node_TypeLogicalExpression,
				Value:    &datatypes.Node_Logical_{Logical: 9999},
				Children: []*datatypes.Node{
					comparisonNode(datatypes.Node_ComparisonEqual, tagNode("tag3"), stringNode("val3")),
					comparisonNode(datatypes.Node_ComparisonEqual, tagNode("tag3"), stringNode("val3")),
				},
			}),
		},

		{
			Name:      "Invalid Root Node",
			Predicate: predicate(tagNode("tag3")),
		},
	}

	for _, test := range cases {
		t.Run(test.Name, func(t *testing.T) {
			_, err := NewProtobufPredicate(test.Predicate)
			if err == nil {
				t.Fatal("expected compile failure")
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
			comparisonNode(datatypes.Node_ComparisonEqual, tagNode("tag5"), stringNode("val5")),
		))
	})

	b.Run("Compound", func(b *testing.B) {
		run(b, predicate(
			orNode(
				andNode(
					comparisonNode(datatypes.Node_ComparisonEqual, tagNode("tag0"), stringNode("val0")),
					comparisonNode(datatypes.Node_ComparisonEqual, tagNode("tag6"), stringNode("val5")),
				),
				comparisonNode(datatypes.Node_ComparisonEqual, tagNode("tag5"), stringNode("val5")),
			),
		))
	})
}

//
// Helpers to create predicate protobufs
//

func tagNode(s string) *datatypes.Node {
	return &datatypes.Node{
		NodeType: datatypes.Node_TypeTagRef,
		Value:    &datatypes.Node_TagRefValue{TagRefValue: s},
	}
}

func stringNode(s string) *datatypes.Node {
	return &datatypes.Node{
		NodeType: datatypes.Node_TypeLiteral,
		Value:    &datatypes.Node_StringValue{StringValue: s},
	}
}

func regexNode(s string) *datatypes.Node {
	return &datatypes.Node{
		NodeType: datatypes.Node_TypeLiteral,
		Value:    &datatypes.Node_RegexValue{RegexValue: s},
	}
}

func comparisonNode(comp datatypes.Node_Comparison, left, right *datatypes.Node) *datatypes.Node {
	return &datatypes.Node{
		NodeType: datatypes.Node_TypeComparisonExpression,
		Value:    &datatypes.Node_Comparison_{Comparison: comp},
		Children: []*datatypes.Node{left, right},
	}
}

func andNode(left, right *datatypes.Node) *datatypes.Node {
	return &datatypes.Node{
		NodeType: datatypes.Node_TypeLogicalExpression,
		Value:    &datatypes.Node_Logical_{Logical: datatypes.Node_LogicalAnd},
		Children: []*datatypes.Node{left, right},
	}
}

func orNode(left, right *datatypes.Node) *datatypes.Node {
	return &datatypes.Node{
		NodeType: datatypes.Node_TypeLogicalExpression,
		Value:    &datatypes.Node_Logical_{Logical: datatypes.Node_LogicalOr},
		Children: []*datatypes.Node{left, right},
	}
}

func predicate(root *datatypes.Node) *datatypes.Predicate {
	return &datatypes.Predicate{Root: root}
}
