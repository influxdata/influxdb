package influxql

import (
	"fmt"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/stdlib/universe"
	"github.com/influxdata/influxql"
)

type joinCursor struct {
	id    flux.OperationID
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
	tables := make(map[flux.OperationID]string)
	for i, cur := range cursors {
		// Record this incoming cursor within the table.
		tableName := fmt.Sprintf("t%d", i)
		tables[cur.ID()] = tableName

		for _, k := range cur.Keys() {
			// Combine the table name with the name to access this attribute so we can know
			// what it will be mapped to.
			varName, _ := cur.Value(k)
			name := fmt.Sprintf("%s_%s", tableName, varName)
			exprs = append(exprs, k)
			m[k] = name
		}
	}

	// Retrieve the parent ids from the cursors.
	parents := make([]flux.OperationID, 0, len(tables))
	for _, cur := range cursors {
		parents = append(parents, cur.ID())
	}
	id := t.op("join", &universe.JoinOpSpec{
		TableNames: tables,
		On:         on,
	}, parents...)
	return &joinCursor{
		id:    id,
		m:     m,
		exprs: exprs,
	}
}

func (c *joinCursor) ID() flux.OperationID {
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
