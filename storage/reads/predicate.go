package reads

import (
	"bytes"
	"strconv"

	"github.com/influxdata/influxdb/storage/reads/datatypes"
)

// NodeVisitor can be called by Walk to traverse the Node hierarchy.
// The Visit() function is called once per node.
type NodeVisitor interface {
	Visit(*datatypes.Node) NodeVisitor
}

func WalkChildren(v NodeVisitor, node *datatypes.Node) {
	for _, n := range node.Children {
		WalkNode(v, n)
	}
}

func WalkNode(v NodeVisitor, node *datatypes.Node) {
	if v = v.Visit(node); v == nil {
		return
	}

	WalkChildren(v, node)
}

func PredicateToExprString(p *datatypes.Predicate) string {
	if p == nil {
		return "[none]"
	}

	var v predicateExpressionPrinter
	WalkNode(&v, p.Root)
	return v.Buffer.String()
}

type predicateExpressionPrinter struct {
	bytes.Buffer
}

func (v *predicateExpressionPrinter) Visit(n *datatypes.Node) NodeVisitor {
	switch n.NodeType {
	case datatypes.Node_TypeLogicalExpression:
		if len(n.Children) > 0 {
			var op string
			if n.GetLogical() == datatypes.Node_LogicalAnd {
				op = " AND "
			} else {
				op = " OR "
			}
			WalkNode(v, n.Children[0])
			for _, e := range n.Children[1:] {
				v.Buffer.WriteString(op)
				WalkNode(v, e)
			}
		}

		return nil

	case datatypes.Node_TypeParenExpression:
		if len(n.Children) == 1 {
			v.Buffer.WriteString("( ")
			WalkNode(v, n.Children[0])
			v.Buffer.WriteString(" )")
		}

		return nil

	case datatypes.Node_TypeComparisonExpression:
		WalkNode(v, n.Children[0])
		v.Buffer.WriteByte(' ')
		switch n.GetComparison() {
		case datatypes.Node_ComparisonEqual:
			v.Buffer.WriteByte('=')
		case datatypes.Node_ComparisonNotEqual:
			v.Buffer.WriteString("!=")
		case datatypes.Node_ComparisonStartsWith:
			v.Buffer.WriteString("startsWith")
		case datatypes.Node_ComparisonRegex:
			v.Buffer.WriteString("=~")
		case datatypes.Node_ComparisonNotRegex:
			v.Buffer.WriteString("!~")
		case datatypes.Node_ComparisonLess:
			v.Buffer.WriteByte('<')
		case datatypes.Node_ComparisonLessEqual:
			v.Buffer.WriteString("<=")
		case datatypes.Node_ComparisonGreater:
			v.Buffer.WriteByte('>')
		case datatypes.Node_ComparisonGreaterEqual:
			v.Buffer.WriteString(">=")
		}

		v.Buffer.WriteByte(' ')
		WalkNode(v, n.Children[1])
		return nil

	case datatypes.Node_TypeTagRef:
		v.Buffer.WriteByte('\'')
		v.Buffer.WriteString(n.GetTagRefValue())
		v.Buffer.WriteByte('\'')
		return nil

	case datatypes.Node_TypeFieldRef:
		v.Buffer.WriteByte('$')
		return nil

	case datatypes.Node_TypeLiteral:
		switch val := n.Value.(type) {
		case *datatypes.Node_StringValue:
			v.Buffer.WriteString(strconv.Quote(val.StringValue))

		case *datatypes.Node_RegexValue:
			v.Buffer.WriteByte('/')
			v.Buffer.WriteString(val.RegexValue)
			v.Buffer.WriteByte('/')

		case *datatypes.Node_IntegerValue:
			v.Buffer.WriteString(strconv.FormatInt(val.IntegerValue, 10))

		case *datatypes.Node_UnsignedValue:
			v.Buffer.WriteString(strconv.FormatUint(val.UnsignedValue, 10))

		case *datatypes.Node_FloatValue:
			v.Buffer.WriteString(strconv.FormatFloat(val.FloatValue, 'f', 10, 64))

		case *datatypes.Node_BooleanValue:
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
