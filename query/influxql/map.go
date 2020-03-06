package influxql

import (
	"fmt"
	"strings"
	"time"

	"github.com/influxdata/flux/ast"
	"github.com/influxdata/influxql"
)

// mapCursor holds the mapping of expressions to specific fields that happens at the end of
// the transpilation.
// TODO(jsternberg): This abstraction might be useful for subqueries, but we only need the expr
// at the moment so just hold that.
type mapCursor struct {
	expr ast.Expression
}

func (c *mapCursor) Expr() ast.Expression {
	return c.expr
}

func (c *mapCursor) Keys() []influxql.Expr {
	panic("unimplemented")
}

func (c *mapCursor) Value(expr influxql.Expr) (string, bool) {
	panic("unimplemented")
}

// mapFields will take the list of symbols and maps each of the operations
// using the column names.
func (t *transpilerState) mapFields(in cursor) (cursor, error) {
	columns := t.stmt.ColumnNames()
	if len(columns) != len(t.stmt.Fields) {
		// TODO(jsternberg): This scenario should not be possible. Replace the use of ColumnNames with a more
		// statically verifiable list of columns when we process the fields from the select statement instead
		// of doing this in the future.
		panic("number of columns does not match the number of fields")
	}

	properties := make([]*ast.Property, 0, len(t.stmt.Fields))
	for i, f := range t.stmt.Fields {
		if ref, ok := f.Expr.(*influxql.VarRef); ok && ref.Val == "time" {
			// Skip past any time columns.
			continue
		}
		fieldName, err := t.mapField(f.Expr, in, false)
		if err != nil {
			return nil, err
		}
		properties = append(properties, &ast.Property{
			Key:   fieldName.(ast.PropertyKey),
			Value: &ast.StringLiteral{Value: columns[i]},
		})
	}
	return &mapCursor{
		expr: &ast.PipeExpression{
			Argument: in.Expr(),
			Call: &ast.CallExpression{
				Callee: &ast.Identifier{
					Name: "rename",
				},
				Arguments: []ast.Expression{
					&ast.ObjectExpression{
						Properties: []*ast.Property{{
							Key: &ast.Identifier{
								Name: "columns",
							},
							Value: &ast.ObjectExpression{
								Properties: properties,
							},
						}},
					},
				},
			},
		},
	}, nil
}

func (t *transpilerState) mapField(expr influxql.Expr, in cursor, returnMemberExpr bool) (ast.Expression, error) {
	if sym, ok := in.Value(expr); ok {
		var mappedName ast.Expression
		if strings.HasPrefix(sym, "_") {
			mappedName = &ast.Identifier{Name: sym}
		} else {
			mappedName = &ast.StringLiteral{Value: sym}
		}
		if returnMemberExpr {
			return &ast.MemberExpression{
				Object:   &ast.Identifier{Name: "r"},
				Property: mappedName.(ast.PropertyKey),
			}, nil
		}
		return mappedName, nil
	}

	switch expr := expr.(type) {
	case *influxql.Call:
		if isMathFunction(expr) {
			return nil, fmt.Errorf("unimplemented math function: %q", expr.Name)
		}
		return nil, fmt.Errorf("missing symbol for %s", expr)
	case *influxql.VarRef:
		return nil, fmt.Errorf("missing symbol for %s", expr)
	case *influxql.BinaryExpr:
		return t.evalBinaryExpr(expr, in)
	case *influxql.ParenExpr:
		return t.mapField(expr.Expr, in, returnMemberExpr)
	case *influxql.StringLiteral:
		if ts, err := expr.ToTimeLiteral(time.UTC); err == nil {
			return &ast.DateTimeLiteral{Value: ts.Val}, nil
		}
		return &ast.StringLiteral{Value: expr.Val}, nil
	case *influxql.NumberLiteral:
		return &ast.FloatLiteral{Value: expr.Val}, nil
	case *influxql.IntegerLiteral:
		return &ast.IntegerLiteral{Value: expr.Val}, nil
	case *influxql.BooleanLiteral:
		return &ast.BooleanLiteral{Value: expr.Val}, nil
	case *influxql.DurationLiteral:
		return &ast.DurationLiteral{
			Values: durationLiteral(expr.Val),
		}, nil
	case *influxql.TimeLiteral:
		return &ast.DateTimeLiteral{Value: expr.Val}, nil
	case *influxql.RegexLiteral:
		return &ast.RegexpLiteral{Value: expr.Val}, nil
	default:
		// TODO(jsternberg): Handle the other expressions by turning them into
		// an equivalent expression.
		return nil, fmt.Errorf("unimplemented: %T", expr)
	}
}

func (t *transpilerState) evalBinaryExpr(expr *influxql.BinaryExpr, in cursor) (ast.Expression, error) {
	fn := func() func(left, right ast.Expression) ast.Expression {
		b := evalBuilder{}
		switch expr.Op {
		case influxql.EQ:
			return b.eval(ast.EqualOperator)
		case influxql.NEQ:
			return b.eval(ast.NotEqualOperator)
		case influxql.GT:
			return b.eval(ast.GreaterThanOperator)
		case influxql.GTE:
			return b.eval(ast.GreaterThanEqualOperator)
		case influxql.LT:
			return b.eval(ast.LessThanOperator)
		case influxql.LTE:
			return b.eval(ast.LessThanEqualOperator)
		case influxql.ADD:
			return b.eval(ast.AdditionOperator)
		case influxql.SUB:
			return b.eval(ast.SubtractionOperator)
		case influxql.AND:
			return b.logical(ast.AndOperator)
		case influxql.OR:
			return b.logical(ast.OrOperator)
		case influxql.EQREGEX:
			return b.eval(ast.RegexpMatchOperator)
		case influxql.NEQREGEX:
			return b.eval(ast.NotRegexpMatchOperator)
		default:
			return nil
		}
	}()
	if fn == nil {
		return nil, fmt.Errorf("unimplemented binary expression: %s", expr.Op)
	}

	lhs, err := t.mapField(expr.LHS, in, true)
	if err != nil {
		return nil, err
	}
	rhs, err := t.mapField(expr.RHS, in, true)
	if err != nil {
		return nil, err
	}
	return fn(lhs, rhs), nil
}

// evalBuilder is used for namespacing the logical and eval wrapping functions.
type evalBuilder struct{}

func (evalBuilder) logical(op ast.LogicalOperatorKind) func(left, right ast.Expression) ast.Expression {
	return func(left, right ast.Expression) ast.Expression {
		return &ast.LogicalExpression{
			Operator: op,
			Left:     left,
			Right:    right,
		}
	}
}

func (evalBuilder) eval(op ast.OperatorKind) func(left, right ast.Expression) ast.Expression {
	return func(left, right ast.Expression) ast.Expression {
		return &ast.BinaryExpression{
			Operator: op,
			Left:     left,
			Right:    right,
		}
	}
}
