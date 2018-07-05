package influxql

import (
	"errors"

	"github.com/influxdata/influxql"
	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/ast"
	"github.com/influxdata/platform/query/execute"
	"github.com/influxdata/platform/query/functions"
	"github.com/influxdata/platform/query/semantic"
)

// cursor is holds known information about the current stream. It maps the influxql ast information
// to the attributes on a table.
type cursor interface {
	// ID contains the last id that produces this cursor.
	ID() query.OperationID

	// Keys returns all of the expressions that this cursor contains.
	Keys() []influxql.Expr

	// Value returns the string that can be used to access the computed expression.
	// If this cursor does not produce the expression, this returns false for the second
	// return argument.
	Value(expr influxql.Expr) (string, bool)
}

// varRefCursor contains a cursor for a single variable. This is usually the raw value
// coming from the database and points to the default value column property.
type varRefCursor struct {
	id  query.OperationID
	ref *influxql.VarRef
}

// createVarRefCursor creates a new cursor from a variable reference using the sources
// in the transpilerState.
func createVarRefCursor(t *transpilerState, ref *influxql.VarRef) (cursor, error) {
	if len(t.stmt.Sources) != 1 {
		// TODO(jsternberg): Support multiple sources.
		return nil, errors.New("unimplemented: only one source is allowed")
	}

	// Only support a direct measurement. Subqueries are not supported yet.
	mm, ok := t.stmt.Sources[0].(*influxql.Measurement)
	if !ok {
		return nil, errors.New("unimplemented: source must be a measurement")
	}

	// Create the from spec and add it to the list of operations.
	from, err := t.from(mm)
	if err != nil {
		return nil, err
	}

	valuer := influxql.NowValuer{Now: t.now}
	_, tr, err := influxql.ConditionExpr(t.stmt.Condition, &valuer)
	if err != nil {
		return nil, err
	}

	// If the maximum is not set and we have a windowing function, then
	// the end time will be set to now.
	if tr.Max.IsZero() {
		if window, err := t.stmt.GroupByInterval(); err == nil && window > 0 {
			tr.Max = t.now
		}
	}

	range_ := t.op("range", &functions.RangeOpSpec{
		Start: query.Time{Absolute: tr.MinTime()},
		Stop:  query.Time{Absolute: tr.MaxTime()},
	}, from)

	id := t.op("filter", &functions.FilterOpSpec{
		Fn: &semantic.FunctionExpression{
			Params: []*semantic.FunctionParam{
				{Key: &semantic.Identifier{Name: "r"}},
			},
			Body: &semantic.LogicalExpression{
				Operator: ast.AndOperator,
				Left: &semantic.BinaryExpression{
					Operator: ast.EqualOperator,
					Left: &semantic.MemberExpression{
						Object:   &semantic.IdentifierExpression{Name: "r"},
						Property: "_measurement",
					},
					Right: &semantic.StringLiteral{Value: mm.Name},
				},
				Right: &semantic.BinaryExpression{
					Operator: ast.EqualOperator,
					Left: &semantic.MemberExpression{
						Object:   &semantic.IdentifierExpression{Name: "r"},
						Property: "_field",
					},
					Right: &semantic.StringLiteral{Value: ref.Val},
				},
			},
		},
	}, range_)
	return &varRefCursor{
		id:  id,
		ref: ref,
	}, nil
}

func (c *varRefCursor) ID() query.OperationID {
	return c.id
}

func (c *varRefCursor) Keys() []influxql.Expr {
	return []influxql.Expr{c.ref}
}

func (c *varRefCursor) Value(expr influxql.Expr) (string, bool) {
	ref, ok := expr.(*influxql.VarRef)
	if !ok {
		return "", false
	}

	// If these are the same variable reference (by pointer), then they are equal.
	if ref == c.ref || *ref == *c.ref {
		return execute.DefaultValueColLabel, true
	}
	return "", false
}

// opCursor wraps a cursor with a new id while delegating all calls to the
// wrapped cursor.
type opCursor struct {
	id query.OperationID
	cursor
}

func (c *opCursor) ID() query.OperationID { return c.id }
