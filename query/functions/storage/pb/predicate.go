package pb

import (
	"fmt"

	"github.com/influxdata/ifql/ast"
	"github.com/influxdata/ifql/semantic"
	"github.com/pkg/errors"
)

func ToStoragePredicate(f *semantic.FunctionExpression) (*Predicate, error) {
	if len(f.Params) != 1 {
		return nil, errors.New("storage predicate functions must have exactly one parameter")
	}

	root, err := toStoragePredicate(f.Body.(semantic.Expression), f.Params[0].Key.Name)
	if err != nil {
		return nil, err
	}

	return &Predicate{
		Root: root,
	}, nil
}

func toStoragePredicate(n semantic.Expression, objectName string) (*Node, error) {
	switch n := n.(type) {
	case *semantic.LogicalExpression:
		left, err := toStoragePredicate(n.Left, objectName)
		if err != nil {
			return nil, errors.Wrap(err, "left hand side")
		}
		right, err := toStoragePredicate(n.Right, objectName)
		if err != nil {
			return nil, errors.Wrap(err, "right hand side")
		}
		children := []*Node{left, right}
		switch n.Operator {
		case ast.AndOperator:
			return &Node{
				NodeType: NodeTypeLogicalExpression,
				Value:    &Node_Logical_{Logical: LogicalAnd},
				Children: children,
			}, nil
		case ast.OrOperator:
			return &Node{
				NodeType: NodeTypeLogicalExpression,
				Value:    &Node_Logical_{Logical: LogicalOr},
				Children: children,
			}, nil
		default:
			return nil, fmt.Errorf("unknown logical operator %v", n.Operator)
		}
	case *semantic.BinaryExpression:
		left, err := toStoragePredicate(n.Left, objectName)
		if err != nil {
			return nil, errors.Wrap(err, "left hand side")
		}
		right, err := toStoragePredicate(n.Right, objectName)
		if err != nil {
			return nil, errors.Wrap(err, "right hand side")
		}
		children := []*Node{left, right}
		op, err := toComparisonOperator(n.Operator)
		if err != nil {
			return nil, err
		}
		return &Node{
			NodeType: NodeTypeComparisonExpression,
			Value:    &Node_Comparison_{Comparison: op},
			Children: children,
		}, nil
	case *semantic.StringLiteral:
		return &Node{
			NodeType: NodeTypeLiteral,
			Value: &Node_StringValue{
				StringValue: n.Value,
			},
		}, nil
	case *semantic.IntegerLiteral:
		return &Node{
			NodeType: NodeTypeLiteral,
			Value: &Node_IntegerValue{
				IntegerValue: n.Value,
			},
		}, nil
	case *semantic.BooleanLiteral:
		return &Node{
			NodeType: NodeTypeLiteral,
			Value: &Node_BooleanValue{
				BooleanValue: n.Value,
			},
		}, nil
	case *semantic.FloatLiteral:
		return &Node{
			NodeType: NodeTypeLiteral,
			Value: &Node_FloatValue{
				FloatValue: n.Value,
			},
		}, nil
	case *semantic.RegexpLiteral:
		return &Node{
			NodeType: NodeTypeLiteral,
			Value: &Node_RegexValue{
				RegexValue: n.Value.String(),
			},
		}, nil
	case *semantic.MemberExpression:
		// Sanity check that the object is the objectName identifier
		if ident, ok := n.Object.(*semantic.IdentifierExpression); !ok || ident.Name != objectName {
			return nil, fmt.Errorf("unknown object %q", n.Object)
		}
		if n.Property == "_value" {
			return &Node{
				NodeType: NodeTypeFieldRef,
				Value: &Node_FieldRefValue{
					FieldRefValue: "_value",
				},
			}, nil
		}
		return &Node{
			NodeType: NodeTypeTagRef,
			Value: &Node_TagRefValue{
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

func toComparisonOperator(o ast.OperatorKind) (Node_Comparison, error) {
	switch o {
	case ast.EqualOperator:
		return ComparisonEqual, nil
	case ast.NotEqualOperator:
		return ComparisonNotEqual, nil
	case ast.RegexpMatchOperator:
		return ComparisonRegex, nil
	case ast.NotRegexpMatchOperator:
		return ComparisonNotRegex, nil
	case ast.StartsWithOperator:
		return ComparisonStartsWith, nil
	case ast.LessThanOperator:
		return ComparisonLess, nil
	case ast.LessThanEqualOperator:
		return ComparisonLessEqual, nil
	case ast.GreaterThanOperator:
		return ComparisonGreater, nil
	case ast.GreaterThanEqualOperator:
		return ComparisonGreaterEqual, nil
	default:
		return 0, fmt.Errorf("unknown operator %v", o)
	}
}
