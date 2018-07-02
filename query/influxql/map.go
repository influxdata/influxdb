package influxql

import (
	"fmt"
	"time"

	"github.com/influxdata/influxql"
	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/ast"
	"github.com/influxdata/platform/query/execute"
	"github.com/influxdata/platform/query/functions"
	"github.com/influxdata/platform/query/semantic"
)

// mapCursor holds the mapping of expressions to specific fields that happens at the end of
// the transpilation.
// TODO(jsternberg): This abstraction might be useful for subqueries, but we only need the id
// at the moment so just hold that.
type mapCursor struct {
	id query.OperationID
}

func (c *mapCursor) ID() query.OperationID {
	return c.id
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

	properties := make([]*semantic.Property, 0, len(t.stmt.Fields)+1)
	properties = append(properties, &semantic.Property{
		Key: &semantic.Identifier{Name: execute.DefaultTimeColLabel},
		Value: &semantic.MemberExpression{
			Object: &semantic.IdentifierExpression{
				Name: "r",
			},
			Property: execute.DefaultTimeColLabel,
		},
	})
	for i, f := range t.stmt.Fields {
		value, err := t.mapField(f.Expr, in)
		if err != nil {
			return nil, err
		}
		properties = append(properties, &semantic.Property{
			Key:   &semantic.Identifier{Name: columns[i]},
			Value: value,
		})
	}
	id := t.op("map", &functions.MapOpSpec{
		Fn: &semantic.FunctionExpression{
			Params: []*semantic.FunctionParam{{
				Key: &semantic.Identifier{Name: "r"},
			}},
			Body: &semantic.ObjectExpression{
				Properties: properties,
			},
		},
		MergeKey: true,
	}, in.ID())
	return &mapCursor{id: id}, nil
}

func (t *transpilerState) mapField(expr influxql.Expr, in cursor) (semantic.Expression, error) {
	if sym, ok := in.Value(expr); ok {
		return &semantic.MemberExpression{
			Object: &semantic.IdentifierExpression{
				Name: "r",
			},
			Property: sym,
		}, nil
	}

	switch expr := expr.(type) {
	case *influxql.Call, *influxql.VarRef:
		return nil, fmt.Errorf("missing symbol for %s", expr)
	case *influxql.BinaryExpr:
		return t.evalBinaryExpr(expr, in)
	case *influxql.ParenExpr:
		return t.mapField(expr.Expr, in)
	case *influxql.StringLiteral:
		if ts, err := expr.ToTimeLiteral(time.UTC); err == nil {
			return &semantic.DateTimeLiteral{Value: ts.Val}, nil
		}
		return &semantic.StringLiteral{Value: expr.Val}, nil
	case *influxql.NumberLiteral:
		return &semantic.FloatLiteral{Value: expr.Val}, nil
	case *influxql.IntegerLiteral:
		return &semantic.IntegerLiteral{Value: expr.Val}, nil
	case *influxql.BooleanLiteral:
		return &semantic.BooleanLiteral{Value: expr.Val}, nil
	case *influxql.DurationLiteral:
		return &semantic.DurationLiteral{Value: expr.Val}, nil
	case *influxql.TimeLiteral:
		return &semantic.DateTimeLiteral{Value: expr.Val}, nil
	default:
		// TODO(jsternberg): Handle the other expressions by turning them into
		// an equivalent expression.
		return nil, fmt.Errorf("unimplemented: %s", expr)
	}
}

func (t *transpilerState) evalBinaryExpr(expr *influxql.BinaryExpr, in cursor) (semantic.Expression, error) {
	fn := func() func(left, right semantic.Expression) semantic.Expression {
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
		default:
			return nil
		}
	}()
	if fn == nil {
		return nil, fmt.Errorf("unimplemented binary expression: %s", expr)
	}

	lhs, err := t.mapField(expr.LHS, in)
	if err != nil {
		return nil, err
	}
	rhs, err := t.mapField(expr.RHS, in)
	if err != nil {
		return nil, err
	}
	return fn(lhs, rhs), nil
}

// evalBuilder is used for namespacing the logical and eval wrapping functions.
type evalBuilder struct{}

func (evalBuilder) logical(op ast.LogicalOperatorKind) func(left, right semantic.Expression) semantic.Expression {
	return func(left, right semantic.Expression) semantic.Expression {
		return &semantic.LogicalExpression{
			Operator: op,
			Left:     left,
			Right:    right,
		}
	}
}

func (evalBuilder) eval(op ast.OperatorKind) func(left, right semantic.Expression) semantic.Expression {
	return func(left, right semantic.Expression) semantic.Expression {
		return &semantic.BinaryExpression{
			Operator: op,
			Left:     left,
			Right:    right,
		}
	}
}
