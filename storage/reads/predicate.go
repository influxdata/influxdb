package reads

import (
	"bytes"
	"fmt"
	"strconv"

	"github.com/influxdata/flux/ast"
	"github.com/influxdata/flux/semantic"
	"github.com/influxdata/platform/storage/reads/datatypes"
	"github.com/influxdata/platform/tsdb"
	"github.com/pkg/errors"
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
	case datatypes.NodeTypeLogicalExpression:
		if len(n.Children) > 0 {
			var op string
			if n.GetLogical() == datatypes.LogicalAnd {
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

	case datatypes.NodeTypeParenExpression:
		if len(n.Children) == 1 {
			v.Buffer.WriteString("( ")
			WalkNode(v, n.Children[0])
			v.Buffer.WriteString(" )")
		}

		return nil

	case datatypes.NodeTypeComparisonExpression:
		WalkNode(v, n.Children[0])
		v.Buffer.WriteByte(' ')
		switch n.GetComparison() {
		case datatypes.ComparisonEqual:
			v.Buffer.WriteByte('=')
		case datatypes.ComparisonNotEqual:
			v.Buffer.WriteString("!=")
		case datatypes.ComparisonStartsWith:
			v.Buffer.WriteString("startsWith")
		case datatypes.ComparisonRegex:
			v.Buffer.WriteString("=~")
		case datatypes.ComparisonNotRegex:
			v.Buffer.WriteString("!~")
		case datatypes.ComparisonLess:
			v.Buffer.WriteByte('<')
		case datatypes.ComparisonLessEqual:
			v.Buffer.WriteString("<=")
		case datatypes.ComparisonGreater:
			v.Buffer.WriteByte('>')
		case datatypes.ComparisonGreaterEqual:
			v.Buffer.WriteString(">=")
		}

		v.Buffer.WriteByte(' ')
		WalkNode(v, n.Children[1])
		return nil

	case datatypes.NodeTypeTagRef:
		v.Buffer.WriteByte('\'')
		v.Buffer.WriteString(n.GetTagRefValue())
		v.Buffer.WriteByte('\'')
		return nil

	case datatypes.NodeTypeFieldRef:
		v.Buffer.WriteByte('$')
		return nil

	case datatypes.NodeTypeLiteral:
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

func toStoragePredicate(f *semantic.FunctionExpression) (*datatypes.Predicate, error) {
	if len(f.Params) != 1 {
		return nil, errors.New("storage predicate functions must have exactly one parameter")
	}

	root, err := toStoragePredicateHelper(f.Body.(semantic.Expression), f.Params[0].Key.Name)
	if err != nil {
		return nil, err
	}

	return &datatypes.Predicate{
		Root: root,
	}, nil
}

func toStoragePredicateHelper(n semantic.Expression, objectName string) (*datatypes.Node, error) {
	switch n := n.(type) {
	case *semantic.LogicalExpression:
		left, err := toStoragePredicateHelper(n.Left, objectName)
		if err != nil {
			return nil, errors.Wrap(err, "left hand side")
		}
		right, err := toStoragePredicateHelper(n.Right, objectName)
		if err != nil {
			return nil, errors.Wrap(err, "right hand side")
		}
		children := []*datatypes.Node{left, right}
		switch n.Operator {
		case ast.AndOperator:
			return &datatypes.Node{
				NodeType: datatypes.NodeTypeLogicalExpression,
				Value:    &datatypes.Node_Logical_{Logical: datatypes.LogicalAnd},
				Children: children,
			}, nil
		case ast.OrOperator:
			return &datatypes.Node{
				NodeType: datatypes.NodeTypeLogicalExpression,
				Value:    &datatypes.Node_Logical_{Logical: datatypes.LogicalOr},
				Children: children,
			}, nil
		default:
			return nil, fmt.Errorf("unknown logical operator %v", n.Operator)
		}
	case *semantic.BinaryExpression:
		left, err := toStoragePredicateHelper(n.Left, objectName)
		if err != nil {
			return nil, errors.Wrap(err, "left hand side")
		}
		right, err := toStoragePredicateHelper(n.Right, objectName)
		if err != nil {
			return nil, errors.Wrap(err, "right hand side")
		}
		children := []*datatypes.Node{left, right}
		op, err := toComparisonOperator(n.Operator)
		if err != nil {
			return nil, err
		}
		return &datatypes.Node{
			NodeType: datatypes.NodeTypeComparisonExpression,
			Value:    &datatypes.Node_Comparison_{Comparison: op},
			Children: children,
		}, nil
	case *semantic.StringLiteral:
		return &datatypes.Node{
			NodeType: datatypes.NodeTypeLiteral,
			Value: &datatypes.Node_StringValue{
				StringValue: n.Value,
			},
		}, nil
	case *semantic.IntegerLiteral:
		return &datatypes.Node{
			NodeType: datatypes.NodeTypeLiteral,
			Value: &datatypes.Node_IntegerValue{
				IntegerValue: n.Value,
			},
		}, nil
	case *semantic.BooleanLiteral:
		return &datatypes.Node{
			NodeType: datatypes.NodeTypeLiteral,
			Value: &datatypes.Node_BooleanValue{
				BooleanValue: n.Value,
			},
		}, nil
	case *semantic.FloatLiteral:
		return &datatypes.Node{
			NodeType: datatypes.NodeTypeLiteral,
			Value: &datatypes.Node_FloatValue{
				FloatValue: n.Value,
			},
		}, nil
	case *semantic.RegexpLiteral:
		return &datatypes.Node{
			NodeType: datatypes.NodeTypeLiteral,
			Value: &datatypes.Node_RegexValue{
				RegexValue: n.Value.String(),
			},
		}, nil
	case *semantic.MemberExpression:
		const (
			fieldKey       = "_field"
			measurementKey = "_measurement"
			valueKey       = "_value"
		)

		// Sanity check that the object is the objectName identifier
		if ident, ok := n.Object.(*semantic.IdentifierExpression); !ok || ident.Name != objectName {
			return nil, fmt.Errorf("unknown object %q", n.Object)
		}
		switch n.Property {
		case fieldKey:
			return &datatypes.Node{
				NodeType: datatypes.NodeTypeTagRef,
				Value: &datatypes.Node_TagRefValue{
					TagRefValue: tsdb.FieldKeyTagKey,
				},
			}, nil
		case measurementKey:
			return &datatypes.Node{
				NodeType: datatypes.NodeTypeTagRef,
				Value: &datatypes.Node_TagRefValue{
					TagRefValue: tsdb.MeasurementTagKey,
				},
			}, nil
		case valueKey:
			return &datatypes.Node{
				NodeType: datatypes.NodeTypeFieldRef,
				Value: &datatypes.Node_FieldRefValue{
					FieldRefValue: valueKey,
				},
			}, nil

		}
		return &datatypes.Node{
			NodeType: datatypes.NodeTypeTagRef,
			Value: &datatypes.Node_TagRefValue{
				TagRefValue: n.Property,
			},
		}, nil
	case *semantic.DurationLiteral:
		return nil, errors.New("duration literals not supported in storage predicates")
	case *semantic.DateTimeLiteral:
		return nil, errors.New("time literals not supported in storage predicates")
	default:
		return nil, fmt.Errorf("unsupported semantic expression type %T", n)
	}
}

func toComparisonOperator(o ast.OperatorKind) (datatypes.Node_Comparison, error) {
	switch o {
	case ast.EqualOperator:
		return datatypes.ComparisonEqual, nil
	case ast.NotEqualOperator:
		return datatypes.ComparisonNotEqual, nil
	case ast.RegexpMatchOperator:
		return datatypes.ComparisonRegex, nil
	case ast.NotRegexpMatchOperator:
		return datatypes.ComparisonNotRegex, nil
	case ast.StartsWithOperator:
		return datatypes.ComparisonStartsWith, nil
	case ast.LessThanOperator:
		return datatypes.ComparisonLess, nil
	case ast.LessThanEqualOperator:
		return datatypes.ComparisonLessEqual, nil
	case ast.GreaterThanOperator:
		return datatypes.ComparisonGreater, nil
	case ast.GreaterThanEqualOperator:
		return datatypes.ComparisonGreaterEqual, nil
	default:
		return 0, fmt.Errorf("unknown operator %v", o)
	}
}
