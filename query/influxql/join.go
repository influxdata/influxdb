package influxql

import (
	"fmt"

	"github.com/influxdata/flux/ast"
	"github.com/influxdata/influxql"
)

type joinCursor struct {
	expr  ast.Expression
	m     map[influxql.Expr]string
	exprs []influxql.Expr
}

func Join(t *transpilerState, cursors []cursor, on []string) cursor {
	if len(cursors) == 1 {
		return cursors[0]
	}

	// Iterate through each cursor and each expression within each cursor to assign them an id.
	var exprs []influxql.Expr
	m := make(map[influxql.Expr]string)
	tables := make([]*ast.Property, 0, len(cursors))
	for _, cur := range cursors {
		// Perform a variable assignment and use it for the table name.
		ident := t.assignment(cur.Expr())
		tableName := ident.Name
		tables = append(tables, &ast.Property{
			Key: ident,
			Value: &ast.Identifier{
				Name: ident.Name,
			},
		})
		for _, k := range cur.Keys() {
			// Combine the table name with the name to access this attribute so we can know
			// what it will be mapped to.
			varName, _ := cur.Value(k)
			name := fmt.Sprintf("%s_%s", tableName, varName)
			exprs = append(exprs, k)
			m[k] = name
		}
	}

	// Construct the expression for the on parameter.
	onExpr := make([]ast.Expression, 0, len(on))
	for _, name := range on {
		onExpr = append(onExpr, &ast.StringLiteral{
			Value: name,
		})
	}

	expr := &ast.CallExpression{
		Callee: &ast.Identifier{
			Name: "join",
		},
		Arguments: []ast.Expression{
			&ast.ObjectExpression{
				Properties: []*ast.Property{
					{
						Key: &ast.Identifier{Name: "tables"},
						Value: &ast.ObjectExpression{
							Properties: tables,
						},
					},
					{
						Key: &ast.Identifier{Name: "on"},
						Value: &ast.ArrayExpression{
							Elements: onExpr,
						},
					},
				},
			},
		},
	}
	return &joinCursor{
		expr:  expr,
		m:     m,
		exprs: exprs,
	}
}

func (c *joinCursor) Expr() ast.Expression {
	return c.expr
}

func (c *joinCursor) Keys() []influxql.Expr {
	keys := make([]influxql.Expr, 0, len(c.m))
	for k := range c.m {
		keys = append(keys, k)
	}
	return keys
}

func (c *joinCursor) Value(expr influxql.Expr) (string, bool) {
	value, ok := c.m[expr]
	return value, ok
}
