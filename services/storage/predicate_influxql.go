package storage

import (
	"errors"
	"regexp"

	"github.com/influxdata/influxql"
)

var measurementRemap = map[string]string{"_measurement": "_name"}

// NodeToExpr transforms a predicate node to an influxql.Expr.
func NodeToExpr(node *Node, remap map[string]string) (influxql.Expr, error) {
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

func (v *nodeToExprVisitor) Visit(n *Node) NodeVisitor {
	if v.err != nil {
		return nil
	}

	switch n.NodeType {
	case NodeTypeLogicalExpression:
		if len(n.Children) > 1 {
			op := influxql.AND
			if n.GetLogical() == LogicalOr {
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

	case NodeTypeParenExpression:
		if len(n.Children) != 1 {
			v.err = errors.New("ParenExpression expects one child")
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

	case NodeTypeComparisonExpression:
		walkChildren(v, n)

		if len(v.exprs) < 2 {
			v.err = errors.New("ComparisonExpression expects two children")
			return nil
		}

		lhs, rhs := v.pop2()

		be := &influxql.BinaryExpr{LHS: lhs, RHS: rhs}
		switch n.GetComparison() {
		case ComparisonEqual:
			be.Op = influxql.EQ
		case ComparisonNotEqual:
			be.Op = influxql.NEQ
		case ComparisonStartsWith:
			// TODO(sgc): rewrite to anchored RE, as index does not support startsWith yet
			v.err = errors.New("startsWith not implemented")
			return nil
		case ComparisonRegex:
			be.Op = influxql.EQREGEX
		case ComparisonNotRegex:
			be.Op = influxql.NEQREGEX
		case ComparisonLess:
			be.Op = influxql.LT
		case ComparisonLessEqual:
			be.Op = influxql.LTE
		case ComparisonGreater:
			be.Op = influxql.GT
		case ComparisonGreaterEqual:
			be.Op = influxql.GTE
		default:
			v.err = errors.New("invalid comparison operator")
			return nil
		}

		v.exprs = append(v.exprs, be)

		return nil

	case NodeTypeTagRef:
		ref := n.GetTagRefValue()
		if v.remap != nil {
			if nk, ok := v.remap[ref]; ok {
				ref = nk
			}
		}

		v.exprs = append(v.exprs, &influxql.VarRef{Val: ref})
		return nil

	case NodeTypeFieldRef:
		v.exprs = append(v.exprs, &influxql.VarRef{Val: "$"})
		return nil

	case NodeTypeLiteral:
		switch val := n.Value.(type) {
		case *Node_StringValue:
			v.exprs = append(v.exprs, &influxql.StringLiteral{Val: val.StringValue})

		case *Node_RegexValue:
			// TODO(sgc): consider hashing the RegexValue and cache compiled version
			re, err := regexp.Compile(val.RegexValue)
			if err != nil {
				v.err = err
			}
			v.exprs = append(v.exprs, &influxql.RegexLiteral{Val: re})
			return nil

		case *Node_IntegerValue:
			v.exprs = append(v.exprs, &influxql.IntegerLiteral{Val: val.IntegerValue})

		case *Node_UnsignedValue:
			v.exprs = append(v.exprs, &influxql.UnsignedLiteral{Val: val.UnsignedValue})

		case *Node_FloatValue:
			v.exprs = append(v.exprs, &influxql.NumberLiteral{Val: val.FloatValue})

		case *Node_BooleanValue:
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

// HasSingleMeasurementNoOR determines if an index optimisation is available.
//
// Typically the read service will use the query engine to retrieve all field
// keys for all measurements that match the expression, which can be very
// inefficient if it can be proved that only one measurement matches the expression.
//
// This condition is determined when the following is true:
//
//		* there is only one occurrence of the tag key `_measurement`.
//		* there are no OR operators in the expression tree.
//		* the operator for the `_measurement` binary expression is ==.
//
func HasSingleMeasurementNoOR(expr influxql.Expr) (string, bool) {
	var lastMeasurement string
	foundOnce := true
	var invalidOP bool

	influxql.WalkFunc(expr, func(node influxql.Node) {
		if !foundOnce || invalidOP {
			return
		}

		if be, ok := node.(*influxql.BinaryExpr); ok {
			if be.Op == influxql.OR {
				invalidOP = true
				return
			}

			if ref, ok := be.LHS.(*influxql.VarRef); ok {
				if ref.Val == measurementRemap[measurementKey] {
					if be.Op != influxql.EQ {
						invalidOP = true
						return
					}

					if lastMeasurement != "" {
						foundOnce = false
					}

					// Check that RHS is a literal string
					if ref, ok := be.RHS.(*influxql.StringLiteral); ok {
						lastMeasurement = ref.Val
					}
				}
			}
		}
	})
	return lastMeasurement, len(lastMeasurement) > 0 && foundOnce && !invalidOP
}

func HasFieldKeyOrValue(expr influxql.Expr) (bool, bool) {
	refs := hasRefs{refs: []string{fieldKey, "$"}, found: make([]bool, 2)}
	influxql.Walk(&refs, expr)
	return refs.found[0], refs.found[1]
}

func RewriteExprRemoveFieldKeyAndValue(expr influxql.Expr) influxql.Expr {
	return influxql.RewriteExpr(expr, func(expr influxql.Expr) influxql.Expr {
		if be, ok := expr.(*influxql.BinaryExpr); ok {
			if ref, ok := be.LHS.(*influxql.VarRef); ok {
				if ref.Val == "_field" || ref.Val == "$" {
					return &influxql.BooleanLiteral{Val: true}
				}
			}
		}

		return expr
	})
}

func RewriteExprRemoveFieldValue(expr influxql.Expr) influxql.Expr {
	return influxql.RewriteExpr(expr, func(expr influxql.Expr) influxql.Expr {
		if be, ok := expr.(*influxql.BinaryExpr); ok {
			if ref, ok := be.LHS.(*influxql.VarRef); ok {
				if ref.Val == "$" {
					return &influxql.BooleanLiteral{Val: true}
				}
			}
		}

		return expr
	})
}
