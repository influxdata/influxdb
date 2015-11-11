package tsdb

import (
	"fmt"
	"time"

	"github.com/influxdb/influxdb/influxql"
	"github.com/influxdb/influxdb/models"
)

// SelectStatementExecutor represents an object that executes SELECT statements.
type SelectStatementExecutor struct {
	stmt *influxql.SelectStatement

	itrs      []Iterator
	startTime time.Time
	endTime   time.Time

	IteratorCreator interface {
		CreateIterator(name string, start, end time.Time) (Iterator, error)
	}
}

// NewSelectStatementExecutor returns an executor for stmt.
func NewSelectStatementExecutor(stmt *influxql.SelectStatement) *SelectStatementExecutor {
	return &SelectStatementExecutor{
		stmt: stmt,
	}
}

// Close stops execution and closes all iterators.
func (e *SelectStatementExecutor) Close() error {
	for _, itr := range e.itrs {
		itr.Close()
	}
	return nil
}

// Execute builds an execution plan and returns a results channel.
func (e *SelectStatementExecutor) Execute() <-chan *models.Row {
	// Determine time range from the condition.
	e.startTime, e.endTime = influxql.TimeRange(e.stmt.Condition)

	// Build iterator tree.
	itrs, err := e.buildIterators()
	if err != nil {
		out := make(chan *models.Row, 1)
		out <- &models.Row{Err: err}
		close(out)
		return out
	}
	e.itrs = itrs

	// Stream from iterators in separate goroutine.
	out := make(chan *models.Row, 0)
	go e.execute(out)
	return out
}

// execute runs in a separate goroutine and sends output rows to the out channel.
func (e *SelectStatementExecutor) execute(out chan *models.Row) {
	defer close(out)

	// Wrap the emitting of results so they are grouped together.
	em := emitter{
		ch:      out,
		columns: e.stmt.ColumnNames(),
	}

	// Continually read from iterators until they are exhausted.
	// Top-level iterators should return an equal number of items.
loop:
	for {
		var name string
		var tags map[string]string
		values := make([]interface{}, len(e.itrs)+1)

		for i, itr := range e.itrs {
			// Read next value from iterator.
			var v Value
			switch itr := itr.(type) {
			case FloatIterator:
				fv := itr.Next()
				if fv == nil {
					break loop
				}
				v = fv
				fmt.Println("?NEXT", v)
			default:
				panic(fmt.Sprintf("unsupported iterator: %T", itr))
			}

			// Use the first iterator to populate name, tags, and timestamp.
			if i == 0 {
				name, tags = valueNameTags(v)
				values[0] = valueTime(v)
			}

			// Set value.
			values[i+1] = valueOf(v)
		}

		// Send row to the results channel.
		em.emit(name, tags, values)
	}

	em.flush()
}

// buildIterators creates an iterator tree from the stmt.
func (e *SelectStatementExecutor) buildIterators() ([]Iterator, error) {
	itrs := make([]Iterator, 0, len(e.stmt.Fields))

	if err := func() error {
		for _, f := range e.stmt.Fields {
			itr, err := e.buildExprIterator(f.Expr)
			if err != nil {
				return err
			}
			itrs = append(itrs, itr)
		}
		return nil

	}(); err != nil {
		Iterators(itrs).Close()
		return nil, err
	}

	return itrs, nil
}

// buildExprIterator creates an iterator for an expression.
func (e *SelectStatementExecutor) buildExprIterator(expr influxql.Expr) (Iterator, error) {
	switch expr := expr.(type) {
	case *influxql.VarRef:
		return e.buildVarRefIterator(expr)
	case *influxql.Call:
		return e.buildCallIterator(expr)
	default:
		panic(fmt.Sprintf("invalid expression type: %T", expr)) // FIXME
	}
}

// buildVarRefIterator creates an iterator for a variable reference.
func (e *SelectStatementExecutor) buildVarRefIterator(ref *influxql.VarRef) (Iterator, error) {
	return e.IteratorCreator.CreateIterator(ref.Val, e.startTime, e.endTime)
}

// buildCallIterator creates an iterator for a function call.
func (e *SelectStatementExecutor) buildCallIterator(c *influxql.Call) (Iterator, error) {
	switch c.Name {
	case "min":
		return e.buildMinIterator(c)
	default:
		return nil, fmt.Errorf("invalid function call: %s", c.Name)
	}
}

// buildMinIterator creates an iterator for a min() call.
func (e *SelectStatementExecutor) buildMinIterator(c *influxql.Call) (Iterator, error) {
	if len(c.Args) != 1 {
		return nil, fmt.Errorf("min: expected 1 arg, got %d", len(c.Args))
	}

	input, err := e.buildExprIterator(c.Args[0])
	if err != nil {
		return nil, err
	}

	switch input := input.(type) {
	case FloatIterator:
		return NewFloatMinIterator([]FloatIterator{input}), nil
	default:
		return &nilFloatIterator{}, nil
	}
}

// emitter groups values together by name,
type emitter struct {
	ch      chan *models.Row
	row     *models.Row
	columns []string
}

// emit appends values to an existing row, if it has matching name and tags.
// Otherwise flushes the emitter and starts a new row.
func (e *emitter) emit(name string, tags map[string]string, values []interface{}) {
	// If there's a row with the same name and tags then append.
	if e.row != nil && e.row.Name != name && tagsEqual(e.row.Tags, tags) {
		e.row.Values = append(e.row.Values, values)
		return
	}

	// If there's a row then emit it.
	if e.row != nil {
		e.flush()
	}

	// Make a new row for the data.
	e.row = &models.Row{
		Name:    name,
		Tags:    tags,
		Columns: e.columns,
		Values:  [][]interface{}{values},
	}
}

// flush sends the current row to the channel.
func (e *emitter) flush() {
	if e.row != nil {
		e.ch <- e.row
		e.ch = nil
	}
}

// tagsEqual returns true if a and b have the same keys and values.
func tagsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}

	for k := range a {
		if a[k] != b[k] {
			return false
		}
	}

	return true
}
