package influxql

import (
	"strings"

	"github.com/influxdata/influxql"
	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/functions"
	"github.com/influxdata/platform/query/semantic"
	"github.com/pkg/errors"
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
	// TODO(jsternberg): Determine which of these cursors are from fields and which are tags.
	var cursors []cursor
	if gr.call != nil {
		ref, ok := gr.call.Args[0].(*influxql.VarRef)
		if !ok {
			// TODO(jsternberg): This should be validated and figured out somewhere else.
			return nil, errors.New("first argument to a function call must be a variable")
		}
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

	// TODO(jsternberg): Establish which variables in the condition are tags and which are fields.
	// We need to create the references to fields here so they can be joined.
	var (
		tags map[influxql.VarRef]struct{}
		cond influxql.Expr
	)
	valuer := influxql.NowValuer{Now: t.now}
	if t.stmt.Condition != nil {
		var err error
		if cond, _, err = influxql.ConditionExpr(t.stmt.Condition, &valuer); err != nil {
			return nil, err
		} else if cond != nil {
			tags = make(map[influxql.VarRef]struct{})

			// Walk through the condition for every variable reference. There will be no function
			// calls here.
			var condErr error
			influxql.WalkFunc(cond, func(node influxql.Node) {
				if condErr != nil {
					return
				}
				ref, ok := node.(*influxql.VarRef)
				if !ok {
					return
				}

				// If the variable reference is in any of the cursors, it is definitely
				// a field and we do not have to inspect it further.
				for _, cur := range cursors {
					if _, ok := cur.Value(ref); ok {
						return
					}
				}

				// This may be a field or a tag. If it is a field, we need to create the cursor
				// and add it to the listing of cursors so it can be joined before we evaluate the condition.
				switch typ := t.mapType(ref); typ {
				case influxql.Tag:
					// Add this variable name to the listing of tags.
					tags[*ref] = struct{}{}
				default:
					cur, err := createVarRefCursor(t, ref)
					if err != nil {
						condErr = err
						return
					}
					cursors = append(cursors, cur)
				}
			})
		}
	}

	// Join the cursors using an inner join.
	// TODO(jsternberg): We need to differentiate between various join types and this needs to be
	// except: ["_field"] rather than joining on the _measurement. This also needs to specify what the time
	// column should be.
	cur := Join(t, cursors, []string{"_measurement"}, nil)
	if len(tags) > 0 {
		cur = &tagsCursor{cursor: cur, tags: tags}
	}

	// Evaluate the conditional and insert a filter if a condition exists.
	if cond != nil {
		// Generate a filter expression by evaluating the condition and wrapping it in a filter op.
		expr, err := t.mapField(cond, cur)
		if err != nil {
			return nil, errors.Wrap(err, "unable to evaluate condition")
		}
		id := t.op("filter", &functions.FilterOpSpec{
			Fn: &semantic.FunctionExpression{
				Params: []*semantic.FunctionParam{{
					Key: &semantic.Identifier{Name: "r"},
				}},
				Body: expr,
			},
		}, cur.ID())
		cur = &opCursor{id: id, cursor: cur}
	}

	// Group together the results.
	if c, err := gr.group(t, cur); err != nil {
		return nil, err
	} else {
		cur = c
	}

	// If a function call is present, evaluate the function call.
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
	// TODO(jsternberg): Process windowing.
	tags := []string{"_measurement"}
	if len(t.stmt.Dimensions) > 0 {
		// Maintain a set of the dimensions we have encountered.
		// This is so we don't duplicate groupings, but we still maintain the
		// listing of tags in the tags slice so it is deterministic.
		m := make(map[string]struct{})
		for _, d := range t.stmt.Dimensions {
			// Reduce the expression before attempting anything. Do not evaluate the call.
			expr := influxql.Reduce(d.Expr, nil)

			switch expr := expr.(type) {
			case *influxql.VarRef:
				if strings.ToLower(expr.Val) == "time" {
					return nil, errors.New("time() is a function and expects at least one argument")
				} else if _, ok := m[expr.Val]; ok {
					continue
				}
				tags = append(tags, expr.Val)
				m[expr.Val] = struct{}{}
			case *influxql.Call:
				// Ensure the call is time() and it has one or two duration arguments.
				if expr.Name != "time" {
					return nil, errors.New("only time() calls allowed in dimensions")
				} else if got := len(expr.Args); got < 1 || got > 2 {
					return nil, errors.New("time dimension expected 1 or 2 arguments")
				} else if _, ok := expr.Args[0].(*influxql.DurationLiteral); !ok {
					return nil, errors.New("time dimension must have duration argument")
				} else {
					return nil, errors.New("unimplemented: windowing support")
				}
			case *influxql.Wildcard:
				return nil, errors.New("unimplemented: dimension wildcards")
			case *influxql.RegexLiteral:
				return nil, errors.New("unimplemented: dimension regex wildcards")
			default:
				return nil, errors.New("only time and tag dimensions allowed")
			}
		}
	}

	// Perform the grouping by the tags we found. There is always a group by because
	// there is always something to group in influxql.
	// TODO(jsternberg): A wildcard will skip this step.
	id := t.op("group", &functions.GroupOpSpec{
		By: tags,
	}, in.ID())

	return &groupCursor{id: id, cursor: in}, nil
}

func (c *groupCursor) ID() query.OperationID { return c.id }

// tagsCursor is a pseudo-cursor that can be used to access tags within the cursor.
type tagsCursor struct {
	cursor
	tags map[influxql.VarRef]struct{}
}

func (c *tagsCursor) Value(expr influxql.Expr) (string, bool) {
	if value, ok := c.cursor.Value(expr); ok {
		return value, ok
	}

	if ref, ok := expr.(*influxql.VarRef); ok {
		if _, ok := c.tags[*ref]; ok {
			return ref.Val, true
		}
	}
	return "", false
}
