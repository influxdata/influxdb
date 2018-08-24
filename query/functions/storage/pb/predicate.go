package pb

import (
	"fmt"

	ostorage "github.com/influxdata/influxdb/services/storage"
	"github.com/influxdata/platform/query/ast"
	"github.com/influxdata/platform/query/semantic"
	"github.com/pkg/errors"
)

func ToStoragePredicate(f *semantic.FunctionExpression) (*ostorage.Predicate, error) {
	if len(f.Params) != 1 {
		return nil, errors.New("storage predicate functions must have exactly one parameter")
	}

	root, err := toStoragePredicate(f.Body.(semantic.Expression), f.Params[0].Key.Name)
	if err != nil {
		return nil, err
	}

	return &ostorage.Predicate{
		Root: root,
	}, nil
}

func toStoragePredicate(n semantic.Expression, objectName string) (*ostorage.Node, error) {
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
		children := []*ostorage.Node{left, right}
		switch n.Operator {
		case ast.AndOperator:
			return &ostorage.Node{
				NodeType: ostorage.NodeTypeLogicalExpression,
				Value:    &ostorage.Node_Logical_{Logical: ostorage.LogicalAnd},
				Children: children,
			}, nil
		case ast.OrOperator:
			return &ostorage.Node{
				NodeType: ostorage.NodeTypeLogicalExpression,
				Value:    &ostorage.Node_Logical_{Logical: ostorage.LogicalOr},
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
		children := []*ostorage.Node{left, right}
		op, err := toComparisonOperator(n.Operator)
		if err != nil {
			return nil, err
		}
		return &ostorage.Node{
			NodeType: ostorage.NodeTypeComparisonExpression,
			Value:    &ostorage.Node_Comparison_{Comparison: op},
			Children: children,
		}, nil
	case *semantic.StringLiteral:
		return &ostorage.Node{
			NodeType: ostorage.NodeTypeLiteral,
			Value: &ostorage.Node_StringValue{
				StringValue: n.Value,
			},
		}, nil
	case *semantic.IntegerLiteral:
		return &ostorage.Node{
			NodeType: ostorage.NodeTypeLiteral,
			Value: &ostorage.Node_IntegerValue{
				IntegerValue: n.Value,
			},
		}, nil
	case *semantic.BooleanLiteral:
		return &ostorage.Node{
			NodeType: ostorage.NodeTypeLiteral,
			Value: &ostorage.Node_BooleanValue{
				BooleanValue: n.Value,
			},
		}, nil
	case *semantic.FloatLiteral:
		return &ostorage.Node{
			NodeType: ostorage.NodeTypeLiteral,
			Value: &ostorage.Node_FloatValue{
				FloatValue: n.Value,
			},
		}, nil
	case *semantic.RegexpLiteral:
		return &ostorage.Node{
			NodeType: ostorage.NodeTypeLiteral,
			Value: &ostorage.Node_RegexValue{
				RegexValue: n.Value.String(),
			},
		}, nil
	case *semantic.MemberExpression:
		// Sanity check that the object is the objectName identifier
		if ident, ok := n.Object.(*semantic.IdentifierExpression); !ok || ident.Name != objectName {
			return nil, fmt.Errorf("unknown object %q", n.Object)
		}
		if n.Property == "_value" {
			return &ostorage.Node{
				NodeType: ostorage.NodeTypeFieldRef,
				Value: &ostorage.Node_FieldRefValue{
					FieldRefValue: "_value",
				},
			}, nil
		}
		return &ostorage.Node{
			NodeType: ostorage.NodeTypeTagRef,
			Value: &ostorage.Node_TagRefValue{
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

func toComparisonOperator(o ast.OperatorKind) (ostorage.Node_Comparison, error) {
	switch o {
	case ast.EqualOperator:
		return ostorage.ComparisonEqual, nil
	case ast.NotEqualOperator:
		return ostorage.ComparisonNotEqual, nil
	case ast.RegexpMatchOperator:
		return ostorage.ComparisonRegex, nil
	case ast.NotRegexpMatchOperator:
		return ostorage.ComparisonNotRegex, nil
	case ast.StartsWithOperator:
		return ostorage.ComparisonStartsWith, nil
	case ast.LessThanOperator:
		return ostorage.ComparisonLess, nil
	case ast.LessThanEqualOperator:
		return ostorage.ComparisonLessEqual, nil
	case ast.GreaterThanOperator:
		return ostorage.ComparisonGreater, nil
	case ast.GreaterThanEqualOperator:
		return ostorage.ComparisonGreaterEqual, nil
	default:
		return 0, fmt.Errorf("unknown operator %v", o)
	}
}
