package storage_test

import (
	"bytes"
	"github.com/influxdata/influxdb/services/storage"
	"strconv"
	"testing"
)

type vis1 struct {
	bytes.Buffer
}

func (v *vis1) Visit(node storage.Node) storage.PredicateVisitor {
	switch n := node.(type) {
	case *storage.Predicate_GroupExpression:
		if len(n.Expressions) > 0 {
			var op string
			if n.Op == storage.LogicalAnd {
				op = " AND "
			} else {
				op = " OR "
			}
			v.Buffer.WriteString("( ")
			storage.Walk(v, n.Expressions[0])
			for _, e := range n.Expressions[1:] {
				v.Buffer.WriteString(op)
				storage.Walk(v, e)
			}

			v.Buffer.WriteString(" )")
		}

		return nil

	case *storage.Predicate_BooleanExpression:
		storage.Walk(v, n.LHS)
		v.Buffer.WriteByte(' ')
		switch n.Op {
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
		storage.Walk(v, n.RHS)
		return nil

	case *storage.Predicate_RefExpression:
		v.Buffer.WriteByte('\'')
		v.Buffer.WriteString(n.Ref)
		v.Buffer.WriteByte('\'')
		return nil

	case *storage.Predicate_LiteralExpression_StringValue:
		v.Buffer.WriteString(strconv.Quote(n.StringValue))
		return nil

	case *storage.Predicate_LiteralExpression_RegexValue:
		v.Buffer.WriteByte('/')
		v.Buffer.WriteString(n.RegexValue)
		v.Buffer.WriteByte('/')
		return nil

	case *storage.Predicate_LiteralExpression_IntegerValue:
		v.Buffer.WriteString(strconv.FormatInt(n.IntegerValue, 10))
		return nil

	case *storage.Predicate_LiteralExpression_UnsignedValue:
		v.Buffer.WriteString(strconv.FormatUint(n.UnsignedValue, 10))
		return nil

	case *storage.Predicate_LiteralExpression_FloatValue:
		v.Buffer.WriteString(strconv.FormatFloat(n.FloatValue, 'f', 10, 64))
		return nil

	case *storage.Predicate_LiteralExpression_BooleanValue:
		if n.BooleanValue {
			v.Buffer.WriteString("true")
		} else {
			v.Buffer.WriteString("false")
		}

		return nil

	default:
		return v
	}
}

func TestWalk(t *testing.T) {
	var v vis1

	pred := &storage.Predicate{
		Root: &storage.Predicate_Expression{
			Value: &storage.Predicate_Expression_GroupExpression{
				GroupExpression: &storage.Predicate_GroupExpression{
					Op: storage.LogicalAnd,
					Expressions: []*storage.Predicate_Expression{
						{
							Value: &storage.Predicate_Expression_BooleanExpression{
								BooleanExpression: &storage.Predicate_BooleanExpression{
									LHS: &storage.Predicate_ValueExpression{
										Value: &storage.Predicate_ValueExpression_Ref{
											Ref: &storage.Predicate_RefExpression{
												Ref: "host",
											},
										},
									},
									Op: storage.ComparisonEqual,
									RHS: &storage.Predicate_LiteralExpression{
										Value: &storage.Predicate_LiteralExpression_StringValue{StringValue: "host1"},
									},
								},
							},
						},
						{
							Value: &storage.Predicate_Expression_BooleanExpression{
								BooleanExpression: &storage.Predicate_BooleanExpression{
									LHS: &storage.Predicate_ValueExpression{
										Value: &storage.Predicate_ValueExpression_Ref{
											Ref: &storage.Predicate_RefExpression{
												Ref: "region",
											},
										},
									},
									Op: storage.ComparisonRegex,
									RHS: &storage.Predicate_LiteralExpression{
										Value: &storage.Predicate_LiteralExpression_RegexValue{RegexValue: "^us-west"},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	storage.Walk(&v, pred)
	t.Log(v.String())
}
