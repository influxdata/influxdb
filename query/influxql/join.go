package influxql

import (
	"fmt"

	"github.com/influxdata/influxql"
	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/execute"
	"github.com/influxdata/platform/query/functions"
	"github.com/influxdata/platform/query/semantic"
)

type joinCursor struct {
	id    query.OperationID
	m     map[influxql.Expr]string
	exprs []influxql.Expr
}

func Join(t *transpilerState, cursors []cursor, on, except []string) cursor {
	if len(cursors) == 1 {
		return cursors[0]
	}

	// Iterate through each cursor and each expression within each cursor to assign them an id.
	var (
		exprs      []influxql.Expr
		properties []*semantic.Property
	)
	m := make(map[influxql.Expr]string)
	tables := make(map[query.OperationID]string)
	for i, cur := range cursors {
		// Record this incoming cursor within the table.
		tableName := fmt.Sprintf("t%d", i)
		tables[cur.ID()] = tableName

		for _, k := range cur.Keys() {
			// Generate a name for accessing this expression and generate the index entries for it.
			name := fmt.Sprintf("val%d", len(exprs))
			exprs = append(exprs, k)
			m[k] = name

			property := &semantic.Property{
				Key: &semantic.Identifier{Name: name},
				Value: &semantic.MemberExpression{
					Object: &semantic.IdentifierExpression{
						Name: "tables",
					},
					Property: tableName,
				},
			}
			if valName, _ := cur.Value(k); valName != execute.DefaultValueColLabel {
				property.Value = &semantic.MemberExpression{
					Object:   property.Value,
					Property: valName,
				}
			}
			properties = append(properties, property)
		}
	}

	// Retrieve the parent tables from the tables map.
	parents := make([]query.OperationID, 0, len(tables))
	for id := range tables {
		parents = append(parents, id)
	}
	id := t.op("join", &functions.JoinOpSpec{
		TableNames: tables,
		Fn: &semantic.FunctionExpression{
			Params: []*semantic.FunctionParam{{
				Key: &semantic.Identifier{Name: "tables"},
			}},
			Body: &semantic.ObjectExpression{
				Properties: properties,
			},
		},
		On: on,
		// TODO(jsternberg): This option needs to be included.
		//Except: except,
	}, parents...)
	return &joinCursor{
		id:    id,
		m:     m,
		exprs: exprs,
	}
}

func (c *joinCursor) ID() query.OperationID {
	return c.id
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
