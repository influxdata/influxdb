package influxql

import (
	"errors"

	"github.com/influxdata/flux/ast"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/influxql"
)

// cursor is holds known information about the current stream. It maps the influxql ast information
// to the attributes on a table.
type cursor interface {
	// Expr is the AST expression that produces this table.
	Expr() ast.Expression

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
	expr ast.Expression
	ref  *influxql.VarRef
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

	valuer := influxql.NowValuer{Now: t.config.Now}
	_, tr, err := influxql.ConditionExpr(t.stmt.Condition, &valuer)
	if err != nil {
		return nil, err
	}

	// If the maximum is not set and we have a windowing function, then
	// the end time will be set to now.
	if tr.Max.IsZero() {
		if window, err := t.stmt.GroupByInterval(); err == nil && window > 0 {
			tr.Max = t.config.Now
		}
	}

	range_ := &ast.PipeExpression{
		Argument: from,
		Call: &ast.CallExpression{
			Callee: &ast.Identifier{
				Name: "range",
			},
			Arguments: []ast.Expression{
				&ast.ObjectExpression{
					Properties: []*ast.Property{
						{
							Key: &ast.Identifier{
								Name: "start",
							},
							Value: &ast.DateTimeLiteral{
								Value: tr.MinTime().UTC(),
							},
						},
						{
							Key: &ast.Identifier{
								Name: "stop",
							},
							Value: &ast.DateTimeLiteral{
								Value: tr.MaxTime().UTC(),
							},
						},
					},
				},
			},
		},
	}

	expr := &ast.PipeExpression{
		Argument: range_,
		Call: &ast.CallExpression{
			Callee: &ast.Identifier{
				Name: "filter",
			},
			Arguments: []ast.Expression{
				&ast.ObjectExpression{
					Properties: []*ast.Property{
						{
							Key: &ast.Identifier{
								Name: "fn",
							},
							Value: &ast.FunctionExpression{
								Params: []*ast.Property{{
									Key: &ast.Identifier{
										Name: "r",
									},
								}},
								Body: &ast.LogicalExpression{
									Operator: ast.AndOperator,
									Left: &ast.BinaryExpression{
										Operator: ast.EqualOperator,
										Left: &ast.MemberExpression{
											Object:   &ast.Identifier{Name: "r"},
											Property: &ast.Identifier{Name: "_measurement"},
										},
										Right: &ast.StringLiteral{
											Value: mm.Name,
										},
									},
									Right: &ast.BinaryExpression{
										Operator: ast.EqualOperator,
										Left: &ast.MemberExpression{
											Object:   &ast.Identifier{Name: "r"},
											Property: &ast.Identifier{Name: "_field"},
										},
										Right: &ast.StringLiteral{
											Value: ref.Val,
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
	return &varRefCursor{
		expr: expr,
		ref:  ref,
	}, nil
}

func (c *varRefCursor) Expr() ast.Expression {
	return c.expr
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

// pipeCursor wraps a cursor with a new expression while delegating all calls to the
// wrapped cursor.
type pipeCursor struct {
	expr ast.Expression
	cursor
}

func (c *pipeCursor) Expr() ast.Expression { return c.expr }
