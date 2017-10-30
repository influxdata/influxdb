package storage

import (
	"math"
	"regexp"

	"github.com/influxdata/influxql"
)

// evalExpr evaluates expr against a map.
func evalExpr(expr influxql.Expr, m valuer) interface{} {
	if expr == nil {
		return nil
	}

	switch expr := expr.(type) {
	case *influxql.BinaryExpr:
		return evalBinaryExpr(expr, m)
	case *influxql.BooleanLiteral:
		return expr.Val
	case *influxql.IntegerLiteral:
		return expr.Val
	case *influxql.UnsignedLiteral:
		return expr.Val
	case *influxql.NumberLiteral:
		return expr.Val
	case *influxql.ParenExpr:
		return evalExpr(expr.Expr, m)
	case *influxql.RegexLiteral:
		return expr.Val
	case *influxql.StringLiteral:
		return expr.Val
	case *influxql.VarRef:
		v, _ := m.Value(expr.Val)
		return v
	default:
		return nil
	}
}

func evalBinaryExpr(expr *influxql.BinaryExpr, m valuer) interface{} {
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
		case influxql.BITWISE_AND:
			return ok && (lhs && rhs)
		case influxql.BITWISE_OR:
			return ok && (lhs || rhs)
		case influxql.BITWISE_XOR:
			return ok && (lhs != rhs)
		case influxql.EQ:
			return ok && (lhs == rhs)
		case influxql.NEQ:
			return ok && (lhs != rhs)
		}
	case float64:
		// Try the rhs as a float64 or int64
		rhsf, ok := rhs.(float64)
		if !ok {
			var rhsi int64
			if rhsi, ok = rhs.(int64); ok {
				rhsf = float64(rhsi)
			}
		}

		rhs := rhsf
		switch expr.Op {
		case influxql.EQ:
			return ok && (lhs == rhs)
		case influxql.NEQ:
			return ok && (lhs != rhs)
		case influxql.LT:
			return ok && (lhs < rhs)
		case influxql.LTE:
			return ok && (lhs <= rhs)
		case influxql.GT:
			return ok && (lhs > rhs)
		case influxql.GTE:
			return ok && (lhs >= rhs)
		case influxql.ADD:
			if !ok {
				return nil
			}
			return lhs + rhs
		case influxql.SUB:
			if !ok {
				return nil
			}
			return lhs - rhs
		case influxql.MUL:
			if !ok {
				return nil
			}
			return lhs * rhs
		case influxql.DIV:
			if !ok {
				return nil
			} else if rhs == 0 {
				return float64(0)
			}
			return lhs / rhs
		case influxql.MOD:
			if !ok {
				return nil
			}
			return math.Mod(lhs, rhs)
		}
	case int64:
		// Try as a float64 to see if a float cast is required.
		rhsf, ok := rhs.(float64)
		if ok {
			lhs := float64(lhs)
			rhs := rhsf
			switch expr.Op {
			case influxql.EQ:
				return lhs == rhs
			case influxql.NEQ:
				return lhs != rhs
			case influxql.LT:
				return lhs < rhs
			case influxql.LTE:
				return lhs <= rhs
			case influxql.GT:
				return lhs > rhs
			case influxql.GTE:
				return lhs >= rhs
			case influxql.ADD:
				return lhs + rhs
			case influxql.SUB:
				return lhs - rhs
			case influxql.MUL:
				return lhs * rhs
			case influxql.DIV:
				if rhs == 0 {
					return float64(0)
				}
				return lhs / rhs
			case influxql.MOD:
				return math.Mod(lhs, rhs)
			}
		} else {
			rhs, ok := rhs.(int64)
			switch expr.Op {
			case influxql.EQ:
				return ok && (lhs == rhs)
			case influxql.NEQ:
				return ok && (lhs != rhs)
			case influxql.LT:
				return ok && (lhs < rhs)
			case influxql.LTE:
				return ok && (lhs <= rhs)
			case influxql.GT:
				return ok && (lhs > rhs)
			case influxql.GTE:
				return ok && (lhs >= rhs)
			case influxql.ADD:
				if !ok {
					return nil
				}
				return lhs + rhs
			case influxql.SUB:
				if !ok {
					return nil
				}
				return lhs - rhs
			case influxql.MUL:
				if !ok {
					return nil
				}
				return lhs * rhs
			case influxql.DIV:
				if !ok {
					return nil
				} else if rhs == 0 {
					return float64(0)
				}
				return lhs / rhs
			case influxql.MOD:
				if !ok {
					return nil
				} else if rhs == 0 {
					return int64(0)
				}
				return lhs % rhs
			case influxql.BITWISE_AND:
				if !ok {
					return nil
				}
				return lhs & rhs
			case influxql.BITWISE_OR:
				if !ok {
					return nil
				}
				return lhs | rhs
			case influxql.BITWISE_XOR:
				if !ok {
					return nil
				}
				return lhs ^ rhs
			}
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

func evalExprBool(expr influxql.Expr, m valuer) bool {
	v, _ := evalExpr(expr, m).(bool)
	return v
}
