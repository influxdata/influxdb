package storage

import (
	"regexp"

	"github.com/influxdata/influxdb/influxql"
)

// evalExpr evaluates expr against a map.
func evalExpr(expr influxql.Expr, m map[string]string) interface{} {
	if expr == nil {
		return nil
	}

	switch expr := expr.(type) {
	case *influxql.BinaryExpr:
		return evalBinaryExpr(expr, m)
	case *influxql.ParenExpr:
		return evalExpr(expr.Expr, m)
	case *influxql.RegexLiteral:
		return expr.Val
	case *influxql.StringLiteral:
		return expr.Val
	case *influxql.VarRef:
		return m[expr.Val]
	default:
		return nil
	}
}

func evalBinaryExpr(expr *influxql.BinaryExpr, m map[string]string) interface{} {
	lhs := evalExpr(expr.LHS, m)
	rhs := evalExpr(expr.RHS, m)
	if lhs == nil && rhs != nil {
		// When the LHS is nil and the RHS is a boolean, implicitly cast the
		// nil to false.
		if _, ok := rhs.(bool); ok {
			lhs = false
		}
	} else if lhs != nil && rhs == nil {
		// Implicit cast of the RHS nil to false when the LHS is a boolean.
		if _, ok := lhs.(bool); ok {
			rhs = false
		}
	}

	// Evaluate if both sides are simple types.
	switch lhs := lhs.(type) {
	case bool:
		rhs, ok := rhs.(bool)
		switch expr.Op {
		case influxql.AND:
			return ok && (lhs && rhs)
		case influxql.OR:
			return ok && (lhs || rhs)
		case influxql.EQ:
			return ok && (lhs == rhs)
		case influxql.NEQ:
			return ok && (lhs != rhs)
		}

	case string:
		switch expr.Op {
		case influxql.EQ:
			rhs, ok := rhs.(string)
			if !ok {
				return nil
			}
			return lhs == rhs
		case influxql.NEQ:
			rhs, ok := rhs.(string)
			if !ok {
				return nil
			}
			return lhs != rhs
		case influxql.EQREGEX:
			rhs, ok := rhs.(*regexp.Regexp)
			if !ok {
				return nil
			}
			return rhs.MatchString(lhs)
		case influxql.NEQREGEX:
			rhs, ok := rhs.(*regexp.Regexp)
			if !ok {
				return nil
			}
			return !rhs.MatchString(lhs)
		}
	}
	return nil
}

func evalExprBool(expr influxql.Expr, m map[string]string) bool {
	v, _ := evalExpr(expr, m).(bool)
	return v
}
