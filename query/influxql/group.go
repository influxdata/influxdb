package influxql

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/influxdata/influxql"
	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/execute"
	"github.com/influxdata/platform/query/functions"
	"github.com/influxdata/platform/query/semantic"
	"github.com/pkg/errors"
)

type groupInfo struct {
	call *influxql.Call
	refs []*influxql.VarRef
}

type groupVisitor struct {
	calls []*function
	refs  []*influxql.VarRef
	err   error
}

func (v *groupVisitor) Visit(n influxql.Node) influxql.Visitor {
	if v.err != nil {
		return nil
	}

	// TODO(jsternberg): Identify duplicates so they are a single common instance.
	switch expr := n.(type) {
	case *influxql.Call:
		// TODO(jsternberg): Identify math functions so we visit their arguments instead of recording them.
		fn, err := parseFunction(expr)
		if err != nil {
			v.err = err
			return nil
		}
		v.calls = append(v.calls, fn)
		return nil
	case *influxql.Distinct:
		v.err = errors.New("unimplemented: distinct expression")
		return nil
	case *influxql.VarRef:
		if expr.Val == "time" {
			return nil
		}
		v.refs = append(v.refs, expr)
		return nil
	case *influxql.Wildcard:
		v.err = errors.New("unimplemented: field wildcard")
		return nil
	case *influxql.RegexLiteral:
		v.err = errors.New("unimplemented: field regex wildcard")
		return nil
	}
	return v
}

// identifyGroups will identify the groups for creating data access cursors.
func identifyGroups(stmt *influxql.SelectStatement) ([]*groupInfo, error) {
	v := &groupVisitor{}
	influxql.Walk(v, stmt.Fields)
	if v.err != nil {
		return nil, v.err
	}

	// Attempt to take the calls and variables and put them into groups.
	if len(v.refs) > 0 {
		// If any of the calls are not selectors, we have an error message.
		for _, fn := range v.calls {
			if !influxql.IsSelector(fn.call) {
				return nil, errors.New("mixing aggregate and non-aggregate queries is not supported")
			}
		}

		// All of the functions are selectors. If we have more than 1, then we have another error message.
		if len(v.calls) > 1 {
			return nil, errors.New("mixing multiple selector functions with tags or fields is not supported")
		}

		// Otherwise, we create a single group.
		var call *influxql.Call
		if len(v.calls) == 1 {
			call = v.calls[0].call
		}
		return []*groupInfo{{
			call: call,
			refs: v.refs,
		}}, nil
	}

	// We do not have any auxiliary fields so each of the function calls goes into
	// its own group.
	groups := make([]*groupInfo, 0, len(v.calls))
	for _, fn := range v.calls {
		groups = append(groups, &groupInfo{call: fn.call})
	}
	return groups, nil
}

func (gr *groupInfo) createCursor(t *transpilerState) (cursor, error) {
	// Create all of the cursors for every variable reference.
	// TODO(jsternberg): Determine which of these cursors are from fields and which are tags.
	var cursors []cursor
	if gr.call != nil {
		ref, ok := gr.call.Args[0].(*influxql.VarRef)
		if !ok {
			// TODO(jsternberg): This should be validated and figured out somewhere else.
			return nil, fmt.Errorf("first argument to %q must be a variable", gr.call.Name)
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
	valuer := influxql.NowValuer{Now: t.spec.Now}
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

	interval, err := t.stmt.GroupByInterval()
	if err != nil {
		return nil, err
	}

	// If a function call is present, evaluate the function call.
	if gr.call != nil {
		c, err := createFunctionCursor(t, gr.call, cur)
		if err != nil {
			return nil, err
		}
		cur = c

		// If there was a window operation, we now need to undo that and sort by the start column
		// so they stay in the same table and are joined in the correct order.
		if interval > 0 {
			cur = &groupCursor{
				id: t.op("window", &functions.WindowOpSpec{
					Every:              query.Duration(math.MaxInt64),
					Period:             query.Duration(math.MaxInt64),
					IgnoreGlobalBounds: true,
					TimeCol:            execute.DefaultTimeColLabel,
					StartColLabel:      execute.DefaultStartColLabel,
					StopColLabel:       execute.DefaultStopColLabel,
				}, cur.ID()),
				cursor: cur,
			}
		}
	} else {
		// If we do not have a function, but we have a field option,
		// return the appropriate error message if there is something wrong with the query.
		if interval > 0 {
			return nil, errors.New("GROUP BY requires at least one aggregate function")
		}

		// TODO(jsternberg): Fill needs to be somewhere and it's probably here somewhere.
		// Move this to the correct location once we've figured it out.
		switch t.stmt.Fill {
		case influxql.NoFill:
			return nil, errors.New("fill(none) must be used with a function")
		case influxql.LinearFill:
			return nil, errors.New("fill(linear) must be used with a function")
		}
	}
	return cur, nil
}

type groupCursor struct {
	cursor
	id query.OperationID
}

func (gr *groupInfo) group(t *transpilerState, in cursor) (cursor, error) {
	var windowEvery time.Duration
	var windowStart time.Time
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
				} else if lit, ok := expr.Args[0].(*influxql.DurationLiteral); !ok {
					return nil, errors.New("time dimension must have duration argument")
				} else if windowEvery != 0 {
					return nil, errors.New("multiple time dimensions not allowed")
				} else {
					windowEvery = lit.Val
					var windowOffset time.Duration
					if len(expr.Args) == 2 {
						switch lit2 := expr.Args[1].(type) {
						case *influxql.DurationLiteral:
							windowOffset = lit2.Val % windowEvery
						case *influxql.TimeLiteral:
							windowOffset = lit2.Val.Sub(lit2.Val.Truncate(windowEvery))
						case *influxql.Call:
							if lit2.Name != "now" {
								return nil, errors.New("time dimension offset function must be now()")
							} else if len(lit2.Args) != 0 {
								return nil, errors.New("time dimension offset now() function requires no arguments")
							}
							now := t.spec.Now
							windowOffset = now.Sub(now.Truncate(windowEvery))

							// Use the evaluated offset to replace the argument. Ideally, we would
							// use the interval assigned above, but the query engine hasn't been changed
							// to use the compiler information yet.
							expr.Args[1] = &influxql.DurationLiteral{Val: windowOffset}
						case *influxql.StringLiteral:
							// If literal looks like a date time then parse it as a time literal.
							if lit2.IsTimeLiteral() {
								t, err := lit2.ToTimeLiteral(t.stmt.Location)
								if err != nil {
									return nil, err
								}
								windowOffset = t.Val.Sub(t.Val.Truncate(windowEvery))
							} else {
								return nil, errors.New("time dimension offset must be duration or now()")
							}
						default:
							return nil, errors.New("time dimension offset must be duration or now()")
						}

						//TODO set windowStart
						windowStart = time.Unix(0, 0).Add(windowOffset)
					}
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

	if windowEvery > 0 {
		windowOp := &functions.WindowOpSpec{
			Every:              query.Duration(windowEvery),
			Period:             query.Duration(windowEvery),
			IgnoreGlobalBounds: true,
			TimeCol:            execute.DefaultTimeColLabel,
			StartColLabel:      execute.DefaultStartColLabel,
			StopColLabel:       execute.DefaultStopColLabel,
		}

		if !windowStart.IsZero() {
			windowOp.Start = query.Time{Absolute: windowStart}
		}

		id = t.op("window", windowOp, id)
	}

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
