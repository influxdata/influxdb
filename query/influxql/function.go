package influxql

import (
	"errors"
	"fmt"

	"github.com/influxdata/flux/ast"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/influxql"
)

// function contains the prototype for invoking a function.
// TODO(jsternberg): This should do a lot more heavy lifting, but it mostly just
// pre-validates that we know the function exists. The cursor creation should be
// done by this struct, but it isn't at the moment.
type function struct {
	Ref  *influxql.VarRef
	call *influxql.Call
}

// parseFunction parses a call AST and creates the function for it.
func parseFunction(expr *influxql.Call) (*function, error) {
	switch expr.Name {
	case "count":
		if exp, got := 1, len(expr.Args); exp != got {
			return nil, fmt.Errorf("invalid number of arguments for %s, expected %d, got %d", expr.Name, exp, got)
		}

		switch ref := expr.Args[0].(type) {
		case *influxql.VarRef:
			return &function{
				Ref:  ref,
				call: expr,
			}, nil
		case *influxql.Call:
			if ref.Name == "distinct" {
				return nil, errors.New("unimplemented: count(distinct)")
			}
			return nil, fmt.Errorf("expected field argument in %s()", expr.Name)
		case *influxql.Distinct:
			return nil, errors.New("unimplemented: count(distinct)")
		case *influxql.Wildcard:
			return nil, errors.New("unimplemented: wildcard function")
		case *influxql.RegexLiteral:
			return nil, errors.New("unimplemented: wildcard regex function")
		default:
			return nil, fmt.Errorf("expected field argument in %s()", expr.Name)
		}
	case "min", "max", "sum", "first", "last", "mean", "median":
		if exp, got := 1, len(expr.Args); exp != got {
			return nil, fmt.Errorf("invalid number of arguments for %s, expected %d, got %d", expr.Name, exp, got)
		}

		switch ref := expr.Args[0].(type) {
		case *influxql.VarRef:
			return &function{
				Ref:  ref,
				call: expr,
			}, nil
		case *influxql.Wildcard:
			return nil, errors.New("unimplemented: wildcard function")
		case *influxql.RegexLiteral:
			return nil, errors.New("unimplemented: wildcard regex function")
		default:
			return nil, fmt.Errorf("expected field argument in %s()", expr.Name)
		}
	case "percentile":
		if exp, got := 2, len(expr.Args); exp != got {
			return nil, fmt.Errorf("invalid number of arguments for %s, expected %d, got %d", expr.Name, exp, got)
		}

		var functionRef *influxql.VarRef

		switch ref := expr.Args[0].(type) {
		case *influxql.VarRef:
			functionRef = ref
		case *influxql.Wildcard:
			return nil, errors.New("unimplemented: wildcard function")
		case *influxql.RegexLiteral:
			return nil, errors.New("unimplemented: wildcard regex function")
		default:
			return nil, fmt.Errorf("expected field argument in %s()", expr.Name)
		}

		switch expr.Args[1].(type) {
		case *influxql.IntegerLiteral:
		case *influxql.NumberLiteral:
		default:
			return nil, fmt.Errorf("expected float argument in %s()", expr.Name)
		}

		return &function{
			Ref:  functionRef,
			call: expr,
		}, nil
	default:
		return nil, fmt.Errorf("unimplemented function: %q", expr.Name)
	}

}

// createFunctionCursor creates a new cursor that calls a function on one of the columns
// and returns the result.
func createFunctionCursor(t *transpilerState, call *influxql.Call, in cursor, normalize bool) (cursor, error) {
	cur := &functionCursor{
		call:   call,
		parent: in,
	}
	switch call.Name {
	case "count", "min", "max", "sum", "first", "last", "mean":
		value, ok := in.Value(call.Args[0])
		if !ok {
			return nil, fmt.Errorf("undefined variable: %s", call.Args[0])
		}
		cur.expr = &ast.PipeExpression{
			Argument: in.Expr(),
			Call: &ast.CallExpression{
				Callee: &ast.Identifier{
					Name: call.Name,
				},
			},
		}
		cur.value = value
		cur.exclude = map[influxql.Expr]struct{}{call.Args[0]: {}}
	case "median":
		value, ok := in.Value(call.Args[0])
		if !ok {
			return nil, fmt.Errorf("undefined variable: %s", call.Args[0])
		}
		cur.expr = &ast.PipeExpression{
			Argument: in.Expr(),
			Call: &ast.CallExpression{
				Callee: &ast.Identifier{
					Name: "percentile",
				},
				Arguments: []ast.Expression{
					&ast.ObjectExpression{
						Properties: []*ast.Property{
							{
								Key: &ast.Identifier{
									Name: "percentile",
								},
								Value: &ast.FloatLiteral{
									Value: 0.5,
								},
							},
							{
								Key: &ast.Identifier{
									Name: "method",
								},
								Value: &ast.StringLiteral{
									Value: "exact_mean",
								},
							},
						},
					},
				},
			},
		}
		cur.value = value
		cur.exclude = map[influxql.Expr]struct{}{call.Args[0]: {}}
	case "percentile":
		if len(call.Args) != 2 {
			return nil, errors.New("percentile function requires two arguments field_key and N")
		}

		fieldName, ok := in.Value(call.Args[0])
		if !ok {
			return nil, fmt.Errorf("undefined variable: %s", call.Args[0])
		}

		var percentile float64
		switch arg := call.Args[1].(type) {
		case *influxql.NumberLiteral:
			percentile = arg.Val / 100.0
		case *influxql.IntegerLiteral:
			percentile = float64(arg.Val) / 100.0
		default:
			return nil, errors.New("argument N must be a float type")
		}

		if percentile < 0 || percentile > 1 {
			return nil, errors.New("argument N must be between 0 and 100")
		}

		args := []*ast.Property{
			{
				Key: &ast.Identifier{
					Name: "percentile",
				},
				Value: &ast.FloatLiteral{
					Value: percentile,
				},
			},
			{
				Key: &ast.Identifier{
					Name: "method",
				},
				Value: &ast.StringLiteral{
					Value: "exact_selector",
				},
			},
		}
		if fieldName != execute.DefaultValueColLabel {
			args = append(args, &ast.Property{
				Key: &ast.Identifier{
					Name: "columns",
				},
				Value: &ast.ArrayExpression{
					Elements: []ast.Expression{
						&ast.StringLiteral{Value: fieldName},
					},
				},
			})
		}
		cur.expr = &ast.PipeExpression{
			Argument: in.Expr(),
			Call: &ast.CallExpression{
				Callee: &ast.Identifier{
					Name: "percentile",
				},
				Arguments: []ast.Expression{
					&ast.ObjectExpression{
						Properties: args,
					},
				},
			},
		}
		cur.value = fieldName
		cur.exclude = map[influxql.Expr]struct{}{call.Args[0]: {}}
	default:
		return nil, fmt.Errorf("unimplemented function: %q", call.Name)
	}

	// If we have been told to normalize the time, we do it here.
	if normalize {
		if influxql.IsSelector(call) {
			cur.expr = &ast.PipeExpression{
				Argument: cur.expr,
				Call: &ast.CallExpression{
					Callee: &ast.Identifier{
						Name: "drop",
					},
					Arguments: []ast.Expression{
						&ast.ObjectExpression{
							Properties: []*ast.Property{{
								Key: &ast.Identifier{
									Name: "columns",
								},
								Value: &ast.ArrayExpression{
									Elements: []ast.Expression{
										&ast.StringLiteral{Value: execute.DefaultTimeColLabel},
									},
								},
							}},
						},
					},
				},
			}
		}
		cur.expr = &ast.PipeExpression{
			Argument: cur.expr,
			Call: &ast.CallExpression{
				Callee: &ast.Identifier{
					Name: "duplicate",
				},
				Arguments: []ast.Expression{
					&ast.ObjectExpression{
						Properties: []*ast.Property{
							{
								Key: &ast.Identifier{
									Name: "column",
								},
								Value: &ast.StringLiteral{
									Value: execute.DefaultStartColLabel,
								},
							},
							{
								Key: &ast.Identifier{
									Name: "as",
								},
								Value: &ast.StringLiteral{
									Value: execute.DefaultTimeColLabel,
								},
							},
						},
					},
				},
			},
		}
	}
	return cur, nil
}

type functionCursor struct {
	expr    ast.Expression
	call    *influxql.Call
	value   string
	exclude map[influxql.Expr]struct{}
	parent  cursor
}

func (c *functionCursor) Expr() ast.Expression {
	return c.expr
}

func (c *functionCursor) Keys() []influxql.Expr {
	keys := []influxql.Expr{c.call}
	if a := c.parent.Keys(); len(a) > 0 {
		for _, e := range a {
			if _, ok := c.exclude[e]; ok {
				continue
			}
			keys = append(keys, e)
		}
	}
	return keys
}

func (c *functionCursor) Value(expr influxql.Expr) (string, bool) {
	if expr == c.call {
		return c.value, true
	} else if _, ok := c.exclude[expr]; ok {
		return "", false
	}
	return c.parent.Value(expr)
}
