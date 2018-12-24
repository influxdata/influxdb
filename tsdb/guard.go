package tsdb

import (
	"bytes"
	"sync"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxql"
)

// guard lets one match a set of points and block until they are done.
type guard struct {
	cond  *sync.Cond
	done  bool
	min   int64
	max   int64
	names map[string]struct{}
	expr  *exprGuard
}

// newGuard constructs a guard that will match any points in the given min and max
// time range, with the given set of measurement names, or the given expression.
// The expression is optional.
func newGuard(min, max int64, names []string, expr influxql.Expr) *guard {
	set := make(map[string]struct{}, len(names))
	for _, name := range names {
		set[name] = struct{}{}
	}
	return &guard{
		cond:  sync.NewCond(new(sync.Mutex)),
		min:   min,
		max:   max,
		names: set,
		expr:  newExprGuard(expr),
	}
}

// Matches returns true if any of the points match the guard.
func (g *guard) Matches(points []models.Point) bool {
	if g == nil {
		return true
	}

	for _, pt := range points {
		if t := pt.Time().UnixNano(); t < g.min || t > g.max {
			continue
		}
		if len(g.names) == 0 && g.expr.matches(pt) {
			return true
		} else if _, ok := g.names[string(pt.Name())]; ok && g.expr.matches(pt) {
			return true
		}
	}
	return false
}

// Wait blocks until the guard has been marked Done.
func (g *guard) Wait() {
	g.cond.L.Lock()
	for !g.done {
		g.cond.Wait()
	}
	g.cond.L.Unlock()
}

// Done signals to anyone waiting on the guard that they can proceed.
func (g *guard) Done() {
	g.cond.L.Lock()
	g.done = true
	g.cond.Broadcast()
	g.cond.L.Unlock()
}

// exprGuard is a union of influxql.Expr based guards. a nil exprGuard matches
// everything, while the zero value matches nothing.
type exprGuard struct {
	and        *[2]*exprGuard
	or         *[2]*exprGuard
	tagMatches *tagGuard
	tagExists  map[string]struct{}
}

type tagGuard struct {
	meas bool
	key  []byte
	op   func([]byte) bool
}

// empty returns true if the exprGuard is empty, meaning that it matches no points.
func (e *exprGuard) empty() bool {
	return e != nil && e.and == nil && e.or == nil && e.tagMatches == nil && e.tagExists == nil
}

// newExprGuard scrutinizes the expression and returns an efficient guard.
func newExprGuard(expr influxql.Expr) *exprGuard {
	if expr == nil {
		return nil
	}

	switch expr := expr.(type) {
	case *influxql.ParenExpr:
		return newExprGuard(expr.Expr)

	case *influxql.BooleanLiteral:
		if expr.Val {
			return nil // matches everything
		}
		return new(exprGuard) // matches nothing

	case *influxql.BinaryExpr:
		switch expr.Op {
		case influxql.AND:
			lhs, rhs := newExprGuard(expr.LHS), newExprGuard(expr.RHS)
			if lhs == nil { // reduce
				return rhs
			} else if rhs == nil { // reduce
				return lhs
			} else if lhs.empty() || rhs.empty() { // short circuit
				return new(exprGuard)
			} else {
				return &exprGuard{and: &[2]*exprGuard{lhs, rhs}}
			}

		case influxql.OR:
			lhs, rhs := newExprGuard(expr.LHS), newExprGuard(expr.RHS)
			if lhs.empty() { // reduce
				return rhs
			} else if rhs.empty() { // reduce
				return lhs
			} else if lhs == nil || rhs == nil { // short circuit
				return nil
			} else {
				return &exprGuard{or: &[2]*exprGuard{lhs, rhs}}
			}

		default:
			return newBinaryExprGuard(expr)
		}
	default:
		// if we couldn't analyze, match everything
		return nil
	}
}

// newBinaryExprGuard scrutinizes the binary expression and returns an efficient guard.
func newBinaryExprGuard(expr *influxql.BinaryExpr) *exprGuard {
	// if it's a nested binary expression, always match.
	if _, ok := expr.LHS.(*influxql.BinaryExpr); ok {
		return nil
	} else if _, ok := expr.RHS.(*influxql.BinaryExpr); ok {
		return nil
	}

	// ensure one of the expressions is a VarRef, and make that the key.
	key, ok := expr.LHS.(*influxql.VarRef)
	value := expr.RHS
	if !ok {
		key, ok = expr.RHS.(*influxql.VarRef)
		if !ok {
			return nil
		}
		value = expr.LHS
	}

	// check the key for situations we know we can't filter.
	if key.Val != "_name" && key.Type != influxql.Unknown && key.Type != influxql.Tag {
		return nil
	}

	// scrutinize the value to return an efficient guard.
	switch value := value.(type) {
	case *influxql.StringLiteral:
		val := []byte(value.Val)
		g := &exprGuard{tagMatches: &tagGuard{
			meas: key.Val == "_name",
			key:  []byte(key.Val),
		}}

		switch expr.Op {
		case influxql.EQ:
			g.tagMatches.op = func(x []byte) bool { return bytes.Equal(val, x) }

		case influxql.NEQ:
			g.tagMatches.op = func(x []byte) bool { return !bytes.Equal(val, x) }

		default: // any other operator isn't valid. conservatively match everything.
			return nil
		}

		return g

	case *influxql.RegexLiteral:
		// There's a tradeoff between being precise and being fast. For example, if the
		// delete includes a very expensive regex, we don't want to run that against every
		// incoming point. The decision here is to match any point that has a possibly
		// expensive match if there is any overlap on the tags. In other words, expensive
		// matches get transformed into trivially matching everything.
		return &exprGuard{tagExists: map[string]struct{}{key.Val: {}}}

	case *influxql.VarRef:
		// We could do a better job here by encoding the two names and checking the points
		// against them, but I'm not quite sure how to do that. Be conservative and match
		// any points that contain either the key or value.

		// since every point has a measurement, always match if either are on the measurement.
		if key.Val == "_name" || value.Val == "_name" {
			return nil
		}
		return &exprGuard{tagExists: map[string]struct{}{
			key.Val:   {},
			value.Val: {},
		}}

	default: // any other value type matches everything
		return nil
	}
}

// matches checks if the exprGuard matches the point.
func (g *exprGuard) matches(pt models.Point) bool {
	switch {
	case g == nil:
		return true

	case g.and != nil:
		return g.and[0].matches(pt) && g.and[1].matches(pt)

	case g.or != nil:
		return g.or[0].matches(pt) || g.or[1].matches(pt)

	case g.tagMatches != nil:
		if g.tagMatches.meas {
			return g.tagMatches.op(pt.Name())
		}
		for _, tag := range pt.Tags() {
			if bytes.Equal(tag.Key, g.tagMatches.key) && g.tagMatches.op(tag.Value) {
				return true
			}
		}
		return false

	case g.tagExists != nil:
		for _, tag := range pt.Tags() {
			if _, ok := g.tagExists[string(tag.Key)]; ok {
				return true
			}
		}
		return false

	default:
		return false
	}
}
