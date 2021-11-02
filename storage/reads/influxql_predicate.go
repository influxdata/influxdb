package reads

import (
	"regexp"

	"github.com/influxdata/influxdb/v2/storage/reads/datatypes"
	"github.com/influxdata/influxql"
	"github.com/pkg/errors"
)

const (
	fieldRef = "$"
)

// NodeToExpr transforms a predicate node to an influxql.Expr.
func NodeToExpr(node *datatypes.Node, remap map[string]string) (influxql.Expr, error) {
	v := &nodeToExprVisitor{remap: remap}
	WalkNode(v, node)
	if err := v.Err(); err != nil {
		return nil, err
	}

	if len(v.exprs) > 1 {
		return nil, errors.New("invalid expression")
	}

	if len(v.exprs) == 0 {
		return nil, nil
	}

	// TODO(edd): It would be preferable if RewriteRegexConditions was a
	// package level function in influxql.
	stmt := &influxql.SelectStatement{
		Condition: v.exprs[0],
	}
	stmt.RewriteRegexConditions()
	return stmt.Condition, nil
}

type nodeToExprVisitor struct {
	remap map[string]string
	exprs []influxql.Expr
	err   error
}

func (v *nodeToExprVisitor) Visit(n *datatypes.Node) NodeVisitor {
	if v.err != nil {
		return nil
	}

	switch n.NodeType {
	case datatypes.Node_TypeLogicalExpression:
		if len(n.Children) > 1 {
			op := influxql.AND
			if n.GetLogical() == datatypes.Node_LogicalOr {
				op = influxql.OR
			}

			WalkNode(v, n.Children[0])
			if v.err != nil {
				return nil
			}

			for i := 1; i < len(n.Children); i++ {
				WalkNode(v, n.Children[i])
				if v.err != nil {
					return nil
				}

				if len(v.exprs) >= 2 {
					lhs, rhs := v.pop2()
					v.exprs = append(v.exprs, &influxql.BinaryExpr{LHS: lhs, Op: op, RHS: rhs})
				}
			}

			return nil
		}

	case datatypes.Node_TypeParenExpression:
		if len(n.Children) != 1 {
			v.err = errors.New("parenExpression expects one child")
			return nil
		}

		WalkNode(v, n.Children[0])
		if v.err != nil {
			return nil
		}

		if len(v.exprs) > 0 {
			v.exprs = append(v.exprs, &influxql.ParenExpr{Expr: v.pop()})
		}

		return nil

	case datatypes.Node_TypeComparisonExpression:
		WalkChildren(v, n)

		if len(v.exprs) < 2 {
			v.err = errors.New("comparisonExpression expects two children")
			return nil
		}

		lhs, rhs := v.pop2()

		be := &influxql.BinaryExpr{LHS: lhs, RHS: rhs}
		switch n.GetComparison() {
		case datatypes.Node_ComparisonEqual:
			be.Op = influxql.EQ
		case datatypes.Node_ComparisonNotEqual:
			be.Op = influxql.NEQ
		case datatypes.Node_ComparisonStartsWith:
			// TODO(sgc): rewrite to anchored RE, as index does not support startsWith yet
			v.err = errors.New("startsWith not implemented")
			return nil
		case datatypes.Node_ComparisonRegex:
			be.Op = influxql.EQREGEX
		case datatypes.Node_ComparisonNotRegex:
			be.Op = influxql.NEQREGEX
		case datatypes.Node_ComparisonLess:
			be.Op = influxql.LT
		case datatypes.Node_ComparisonLessEqual:
			be.Op = influxql.LTE
		case datatypes.Node_ComparisonGreater:
			be.Op = influxql.GT
		case datatypes.Node_ComparisonGreaterEqual:
			be.Op = influxql.GTE
		default:
			v.err = errors.New("invalid comparison operator")
			return nil
		}

		v.exprs = append(v.exprs, be)

		return nil

	case datatypes.Node_TypeTagRef:
		ref := n.GetTagRefValue()
		if v.remap != nil {
			if nk, ok := v.remap[ref]; ok {
				ref = nk
			}
		}

		v.exprs = append(v.exprs, &influxql.VarRef{Val: ref, Type: influxql.Tag})
		return nil

	case datatypes.Node_TypeFieldRef:
		v.exprs = append(v.exprs, &influxql.VarRef{Val: fieldRef})
		return nil

	case datatypes.Node_TypeLiteral:
		switch val := n.Value.(type) {
		case *datatypes.Node_StringValue:
			v.exprs = append(v.exprs, &influxql.StringLiteral{Val: val.StringValue})

		case *datatypes.Node_RegexValue:
			// TODO(sgc): consider hashing the RegexValue and cache compiled version
			re, err := regexp.Compile(val.RegexValue)
			if err != nil {
				v.err = err
			}
			v.exprs = append(v.exprs, &influxql.RegexLiteral{Val: re})
			return nil

		case *datatypes.Node_IntegerValue:
			v.exprs = append(v.exprs, &influxql.IntegerLiteral{Val: val.IntegerValue})

		case *datatypes.Node_UnsignedValue:
			v.exprs = append(v.exprs, &influxql.UnsignedLiteral{Val: val.UnsignedValue})

		case *datatypes.Node_FloatValue:
			v.exprs = append(v.exprs, &influxql.NumberLiteral{Val: val.FloatValue})

		case *datatypes.Node_BooleanValue:
			v.exprs = append(v.exprs, &influxql.BooleanLiteral{Val: val.BooleanValue})

		default:
			v.err = errors.New("unexpected literal type")
			return nil
		}

		return nil

	default:
		return v
	}
	return nil
}

func (v *nodeToExprVisitor) Err() error {
	return v.err
}

func (v *nodeToExprVisitor) pop() influxql.Expr {
	if len(v.exprs) == 0 {
		panic("stack empty")
	}

	var top influxql.Expr
	top, v.exprs = v.exprs[len(v.exprs)-1], v.exprs[:len(v.exprs)-1]
	return top
}

func (v *nodeToExprVisitor) pop2() (influxql.Expr, influxql.Expr) {
	if len(v.exprs) < 2 {
		panic("stack empty")
	}

	rhs := v.exprs[len(v.exprs)-1]
	lhs := v.exprs[len(v.exprs)-2]
	v.exprs = v.exprs[:len(v.exprs)-2]
	return lhs, rhs
}

func IsTrueBooleanLiteral(expr influxql.Expr) bool {
	b, ok := expr.(*influxql.BooleanLiteral)
	if ok {
		return b.Val
	}
	return false
}

func IsFalseBooleanLiteral(expr influxql.Expr) bool {
	b, ok := expr.(*influxql.BooleanLiteral)
	if ok {
		return !b.Val
	}
	return false
}

func RewriteExprRemoveFieldValue(expr influxql.Expr) influxql.Expr {
	return influxql.RewriteExpr(expr, func(expr influxql.Expr) influxql.Expr {
		if be, ok := expr.(*influxql.BinaryExpr); ok {
			if ref, ok := be.LHS.(*influxql.VarRef); ok {
				if ref.Val == fieldRef {
					return &influxql.BooleanLiteral{Val: true}
				}
			}
		}

		return expr
	})
}

type hasRefs struct {
	refs  []string
	found []bool
}

func (v *hasRefs) allFound() bool {
	for _, val := range v.found {
		if !val {
			return false
		}
	}
	return true
}

func (v *hasRefs) Visit(node influxql.Node) influxql.Visitor {
	if v.allFound() {
		return nil
	}

	if n, ok := node.(*influxql.VarRef); ok {
		for i, r := range v.refs {
			if !v.found[i] && r == n.Val {
				v.found[i] = true
				if v.allFound() {
					return nil
				}
			}
		}
	}
	return v
}

func ExprHasKey(expr influxql.Expr, key string) bool {
	refs := hasRefs{refs: []string{key}, found: make([]bool, 1)}
	influxql.Walk(&refs, expr)
	return refs.found[0]
}

func HasFieldValueKey(expr influxql.Expr) bool {
	return ExprHasKey(expr, fieldRef)
}
