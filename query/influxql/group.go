package influxql

import (
	"github.com/influxdata/influxql"
	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/functions"
)

type groupInfo struct {
	call *influxql.Call
	refs []*influxql.VarRef
}

type groupVisitor struct {
	groups []*groupInfo
}

func (v *groupVisitor) Visit(n influxql.Node) influxql.Visitor {
	// TODO(jsternberg): Identify duplicates so they are a single common instance.
	switch expr := n.(type) {
	case *influxql.Call:
		// TODO(jsternberg): Identify math functions so we visit their arguments instead of recording them.
		// If we have a single group, it does not contain a call, and this is a selector, make this
		// the function call for the first group.
		if len(v.groups) > 0 && influxql.IsSelector(expr) && v.groups[0].call == nil {
			v.groups[0].call = expr
		} else {
			// Otherwise, we create a new group and place this expression as the call.
			v.groups = append(v.groups, &groupInfo{call: expr})
		}
		return nil
	case *influxql.VarRef:
		// If we have one group, add this as a variable reference to that group.
		// If we have zero, then create the first group. If there are multiple groups,
		// that's technically a query error, but we'll capture that somewhere else before
		// this (maybe).
		// TODO(jsternberg): Identify invalid queries where an aggregate is used with a raw value.
		if len(v.groups) == 0 {
			v.groups = append(v.groups, &groupInfo{})
		}
		v.groups[0].refs = append(v.groups[0].refs, expr)
		return nil
	}
	return v
}

// identifyGroups will identify the groups for creating data access cursors.
func identifyGroups(stmt *influxql.SelectStatement) []*groupInfo {
	// Try to estimate the number of groups. This isn't a very important step so we
	// don't care if we are wrong. If this is a raw query, then the size is going to be 1.
	// If this is an aggregate, the size will probably be the number of fields.
	// If this is a selector, the size will be 1 again so we'll just get this wrong.
	sizeHint := 1
	if !stmt.IsRawQuery {
		sizeHint = len(stmt.Fields)
	}

	v := &groupVisitor{
		groups: make([]*groupInfo, 0, sizeHint),
	}
	influxql.Walk(v, stmt.Fields)
	return v.groups
}

func (gr *groupInfo) createCursor(t *transpilerState) (cursor, error) {
	// Create all of the cursors for every variable reference.
	var cursors []cursor
	if gr.call != nil {
		ref := gr.call.Args[0].(*influxql.VarRef)
		cur, err := createVarRefCursor(t, ref)
		if err != nil {
			return nil, err
		}
		cursors = append(cursors, cur)
	}

	for _, ref := range gr.refs {
		cur, err := createVarRefCursor(t, ref)
		if err != nil {
			return nil, err
		}
		cursors = append(cursors, cur)
	}

	// Join the cursors using an inner join.
	// TODO(jsternberg): We need to differentiate between various join types and this needs to be
	// except: ["_field"] rather than joining on the _measurement. This also needs to specify what the time
	// column should be.
	cur := Join(t, cursors, []string{"_measurement"}, nil)

	// TODO(jsternberg): Handle conditions, function calls, multiple variable references, and basically
	// everything that needs to be done to create a cursor for a single group.

	if c, err := gr.group(t, cur); err != nil {
		return nil, err
	} else {
		cur = c
	}

	if gr.call != nil {
		c, err := createFunctionCursor(t, gr.call, cur)
		if err != nil {
			return nil, err
		}
		cur = c
	}
	return cur, nil
}

type groupCursor struct {
	cursor
	id query.OperationID
}

func (gr *groupInfo) group(t *transpilerState, in cursor) (cursor, error) {
	// TODO(jsternberg): Process group by clause correctly and windowing.
	id := t.op("group", &functions.GroupOpSpec{
		By: []string{"_measurement"},
	}, in.ID())
	return &groupCursor{id: id, cursor: in}, nil
}

func (c *groupCursor) ID() query.OperationID { return c.id }
