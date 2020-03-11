package influxql

import (
	"errors"
	"fmt"
	"time"

	"github.com/influxdata/flux/ast"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/influxql"
)

func isTransformation(expr influxql.Expr) bool {
	if call, ok := expr.(*influxql.Call); ok {
		switch call.Name {
		// TODO(ethan): more to be added here.
		case "difference", "derivative", "cumulative_sum", "elapsed":
			return true
		}
	}
	return false
}

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
	case "min", "max", "sum", "first", "last", "mean", "median", "difference", "stddev", "spread":
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
	case "count", "min", "max", "sum", "first", "last", "mean", "difference", "stddev", "spread":
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
	case "elapsed":
		// TODO(ethan): https://github.com/influxdata/influxdb/issues/10733 to enable this.
		value, ok := in.Value(call.Args[0])
		if !ok {
			return nil, fmt.Errorf("undefined variable: %s", call.Args[0])
		}
		unit := []ast.Duration{{
			Magnitude: 1,
			Unit:      "ns",
		}}
		// elapsed has an optional unit parameter, default to 1ns
		// https://docs.influxdata.com/influxdb/v1.7/query_language/functions/#elapsed
		if len(call.Args) == 2 {
			switch arg := call.Args[1].(type) {
			case *influxql.DurationLiteral:
				unit = durationLiteral(arg.Val)
			default:
				return nil, errors.New("argument unit must be a duration type")
			}
		}
		cur.expr = &ast.PipeExpression{
			Argument: in.Expr(),
			Call: &ast.CallExpression{
				Callee: &ast.Identifier{
					Name: call.Name,
				},
				Arguments: []ast.Expression{
					&ast.ObjectExpression{
						Properties: []*ast.Property{
							{
								Key: &ast.Identifier{
									Name: "unit",
								},
								Value: &ast.DurationLiteral{
									Values: unit,
								},
							},
						},
					},
				},
			},
		}
		cur.value = value
	case "median":
		value, ok := in.Value(call.Args[0])
		if !ok {
			return nil, fmt.Errorf("undefined variable: %s", call.Args[0])
		}
		cur.expr = &ast.PipeExpression{
			Argument: in.Expr(),
			Call: &ast.CallExpression{
				Callee: &ast.Identifier{
					Name: "median",
				},
				Arguments: []ast.Expression{
					&ast.ObjectExpression{
						Properties: []*ast.Property{
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
					Name: "q",
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
					Name: "quantile",
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
		// err checked in caller
		interval, _ := t.stmt.GroupByInterval()
		var timeValue ast.Expression
		if interval > 0 {
			timeValue = &ast.MemberExpression{
				Object: &ast.Identifier{
					Name: "r",
				},
				Property: &ast.Identifier{
					Name: execute.DefaultStartColLabel,
				},
			}
		} else if isTransformation(call) || influxql.IsSelector(call) {
			timeValue = &ast.MemberExpression{
				Object: &ast.Identifier{
					Name: "r",
				},
				Property: &ast.Identifier{
					Name: execute.DefaultTimeColLabel,
				},
			}
		} else {
			valuer := influxql.NowValuer{Now: t.config.Now}
			_, tr, err := influxql.ConditionExpr(t.stmt.Condition, &valuer)
			if err != nil {
				return nil, err
			}
			if tr.MinTime().UnixNano() == influxql.MinTime {
				timeValue = &ast.DateTimeLiteral{Value: time.Unix(0, 0).UTC()}
			} else {
				timeValue = &ast.MemberExpression{
					Object: &ast.Identifier{
						Name: "r",
					},
					Property: &ast.Identifier{
						Name: execute.DefaultStartColLabel,
					},
				}
			}
		}
		cur.expr = &ast.PipeExpression{
			Argument: cur.expr,
			Call: &ast.CallExpression{
				Callee: &ast.Identifier{
					Name: "map",
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
										Key: &ast.Identifier{Name: "r"},
									}},
									Body: &ast.ObjectExpression{
										With: &ast.Identifier{Name: "r"},
										Properties: []*ast.Property{{
											Key:   &ast.Identifier{Name: execute.DefaultTimeColLabel},
											Value: timeValue,
										}},
									},
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
