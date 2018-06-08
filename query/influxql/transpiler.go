package influxql

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/influxdata/influxql"
	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/functions"
)

// Transpiler converts InfluxQL queries into a query spec.
type Transpiler struct{}

func NewTranspiler() *Transpiler {
	return new(Transpiler)
}

func (t *Transpiler) Transpile(ctx context.Context, txt string) (*query.Spec, error) {
	// Parse the text of the query.
	q, err := influxql.ParseQuery(txt)
	if err != nil {
		return nil, err
	}

	if len(q.Statements) != 1 {
		// TODO(jsternberg): Handle queries with multiple statements.
		return nil, errors.New("unimplemented: only one statement is allowed")
	}

	s, ok := q.Statements[0].(*influxql.SelectStatement)
	if !ok {
		// TODO(jsternberg): Support meta queries.
		return nil, errors.New("only supports select statements")
	}

	transpiler := newTranspilerState(s)
	return transpiler.Transpile(ctx)
}

type transpilerState struct {
	stmt   *influxql.SelectStatement
	spec   *query.Spec
	nextID map[string]int
	now    time.Time
}

func newTranspilerState(stmt *influxql.SelectStatement) *transpilerState {
	state := &transpilerState{
		stmt:   stmt.Clone(),
		spec:   &query.Spec{},
		nextID: make(map[string]int),
		now:    time.Now(),
	}
	// Omit the time from the cloned statement so it doesn't show up in
	// the list of column names.
	state.stmt.OmitTime = true
	return state
}

func (t *transpilerState) Transpile(ctx context.Context) (*query.Spec, error) {
	groups := identifyGroups(t.stmt)
	if len(groups) == 0 {
		return nil, errors.New("no fields")
	}

	cursors := make([]cursor, 0, len(groups))
	for _, gr := range groups {
		cur, err := gr.createCursor(t)
		if err != nil {
			return nil, err
		}
		cursors = append(cursors, cur)
	}

	// Join the cursors together on the measurement name.
	// TODO(jsternberg): This needs to join on all remaining partition keys.
	cur := Join(t, cursors, []string{"_measurement"}, nil)

	// Map each of the fields into another cursor. This evaluates any lingering expressions.
	cur, err := t.mapFields(cur)
	if err != nil {
		return nil, err
	}

	// Yield the cursor from the last cursor to a stream with the name of the statement id.
	// TODO(jsternberg): Include the statement id in the transpiler state when we create
	// the state so we can yield to something other than zero.
	t.op("yield", &functions.YieldOpSpec{Name: "0"}, cur.ID())
	return t.spec, nil
}

func (t *transpilerState) op(name string, spec query.OperationSpec, parents ...query.OperationID) query.OperationID {
	op := query.Operation{
		ID:   query.OperationID(fmt.Sprintf("%s%d", name, t.nextID[name])),
		Spec: spec,
	}
	t.spec.Operations = append(t.spec.Operations, &op)
	for _, pid := range parents {
		t.spec.Edges = append(t.spec.Edges, query.Edge{
			Parent: pid,
			Child:  op.ID,
		})
	}
	t.nextID[name]++
	return op.ID
}
