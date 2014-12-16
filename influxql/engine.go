package influxql

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// DB represents an interface to the underlying storage.
type DB interface {
	// Returns a list of series data ids matching a name and tags.
	MatchSeries(name string, tags map[string]string) []uint32

	// Returns the id and data type for a series field.
	Field(name, field string) (fieldID uint8, typ DataType)

	// Returns an iterator given a series data id, field id, & field data type.
	CreateIterator(id uint32, fieldID uint8, typ DataType, min, max time.Time, interval time.Duration) Iterator
}

// Planner represents an object for creating execution plans.
type Planner struct {
	// The underlying storage that holds series and field meta data.
	DB DB

	// Returns the current time. Defaults to time.Now().
	Now func() time.Time
}

// NewPlanner returns a new instance of Planner.
func NewPlanner(db DB) *Planner {
	return &Planner{
		DB:  db,
		Now: time.Now,
	}
}

func (p *Planner) Plan(stmt *SelectStatement) (*Executor, error) {
	// Create the executor.
	e := &Executor{
		db:         p.DB,
		stmt:       stmt,
		processors: make([]processor, len(stmt.Fields)),
	}

	// Fold conditional.
	now := p.Now()
	stmt.Condition = Fold(stmt.Condition, &now)

	// Extract the time range.
	min, max := TimeRange(stmt.Condition)
	if max.IsZero() {
		max = now
	}
	if max.Before(min) {
		return nil, fmt.Errorf("invalid time range: %s - %s", min.Format(TimeFormat), max.Format(TimeFormat))
	}
	e.min, e.max = min, max

	// Determine group by interval.
	interval, dimensions, err := p.normalizeDimensions(stmt.Dimensions)
	if err != nil {
		return nil, err
	}
	e.interval, e.dimensions = interval, dimensions

	// Generate a processor for each field.
	for i, f := range stmt.Fields {
		p, err := p.planField(e, f)
		if err != nil {
			return nil, err
		}
		e.processors[i] = p
	}

	return e, nil
}

// normalizeDimensions extacts the time interval, if specified.
// Returns all remaining dimensions.
func (p *Planner) normalizeDimensions(dimensions Dimensions) (time.Duration, Dimensions, error) {
	// Ignore if there are no dimensions.
	if len(dimensions) == 0 {
		return 0, nil, nil
	}

	// If the first dimension is a "time(duration)" then extract the duration.
	if call, ok := dimensions[0].Expr.(*Call); ok && strings.ToLower(call.Name) == "time" {
		// Make sure there is exactly one argument.
		if len(call.Args) != 1 {
			return 0, nil, errors.New("time dimension expected one argument")
		}

		// Ensure the argument is a duration.
		lit, ok := call.Args[0].(*DurationLiteral)
		if !ok {
			return 0, nil, errors.New("time dimension must have one duration argument")
		}
		return lit.Val, dimensions[1:], nil
	}

	return 0, dimensions, nil
}

// planField returns a processor for field.
func (p *Planner) planField(e *Executor, f *Field) (processor, error) {
	return p.planExpr(e, f.Expr)
}

// planExpr returns a processor for an expression.
func (p *Planner) planExpr(e *Executor, expr Expr) (processor, error) {
	switch expr := expr.(type) {
	case *VarRef:
		panic("TODO")
	case *Call:
		return p.planCall(e, expr)
	case *BinaryExpr:
		return p.planBinaryExpr(e, expr)
	case *ParenExpr:
		return p.planExpr(e, expr.Expr)
	case *NumberLiteral:
		return newLiteralProcessor(expr.Val), nil
	case *StringLiteral:
		return newLiteralProcessor(expr.Val), nil
	case *BooleanLiteral:
		return newLiteralProcessor(expr.Val), nil
	case *TimeLiteral:
		return newLiteralProcessor(expr.Val), nil
	case *DurationLiteral:
		return newLiteralProcessor(expr.Val), nil
	}
	panic("unreachable")
}

// planCall generates a processor for a function call.
func (p *Planner) planCall(e *Executor, c *Call) (processor, error) {
	// Ensure there is a single argument.
	if len(c.Args) != 1 {
		return nil, fmt.Errorf("expected one argument for %s()", c.Name)
	}

	// Ensure the argument is a variable reference.
	ref, ok := c.Args[0].(*VarRef)
	if !ok {
		return nil, fmt.Errorf("expected field argument in %s()", c.Name)
	}

	// Extract the substatement for the call.
	sub, err := e.stmt.Substatement(ref)
	if err != nil {
		return nil, err
	}
	name := sub.Source.(*Series).Name
	tags := make(map[string]string) // TODO: Extract tags.

	// Find field.
	fname := strings.TrimPrefix(ref.Val, name+".")
	fieldID, typ := e.db.Field(name, fname)
	if fieldID == 0 {
		return nil, fmt.Errorf("field not found: %s.%s", name, fname)
	}

	// Generate a reducer for the given function.
	r := newReducer(e)
	r.stmt = sub

	// Retrieve a list of series data ids.
	seriesIDs := p.DB.MatchSeries(name, tags)

	// Generate mappers for each id.
	r.mappers = make([]*mapper, len(seriesIDs))
	for i, seriesID := range seriesIDs {
		m := newMapper(e, seriesID, fieldID, typ)
		m.min, m.max = e.min.UnixNano(), e.max.UnixNano()
		m.interval = int64(e.interval)
		r.mappers[i] = m
	}

	// Set the appropriate reducer function.
	switch strings.ToLower(c.Name) {
	case "count":
		r.fn = reduceSum
		for _, m := range r.mappers {
			m.fn = mapCount
		}
	case "sum":
		r.fn = reduceSum
		for _, m := range r.mappers {
			m.fn = mapSum
		}
	default:
		return nil, fmt.Errorf("function not found: %q", c.Name)
	}

	return r, nil
}

// planBinaryExpr generates a processor for a binary expression.
// A binary expression represents a join operator between two processors.
func (p *Planner) planBinaryExpr(e *Executor, expr *BinaryExpr) (processor, error) {
	panic("TODO")
}

// Executor represents the implementation of Executor.
// It executes all reducers and combines their result into a row.
type Executor struct {
	db         DB               // source database
	stmt       *SelectStatement // original statement
	processors []processor      // per-field processors
	min, max   time.Time        // time range
	interval   time.Duration    // group by duration
	dimensions Dimensions       // non-interval dimensions
}

// Execute begins execution of the query and returns a channel to receive rows.
func (e *Executor) Execute() (<-chan *Row, error) {
	// Initialize processors.
	for _, p := range e.processors {
		p.start()
	}

	// Create output channel and stream data in a separate goroutine.
	out := make(chan *Row, 0)
	go e.execute(out)

	return out, nil
}

// execute runs in a separate separate goroutine and streams data from processors.
func (e *Executor) execute(out chan *Row) {
	// TODO: Support multi-value rows.

	// Initialize row.
	row := &Row{}
	row.Name = e.processors[0].name()

	// Create column names.
	row.Columns = make([]string, 0)
	if e.interval != 0 {
		row.Columns = append(row.Columns, "time")
	}
	for i, f := range e.stmt.Fields {
		name := f.Name()
		if name == "" {
			name = fmt.Sprintf("col%d", i)
		}
		row.Columns = append(row.Columns, name)
	}

	// Combine values from each processor.
loop:
	for {
		values := make([]interface{}, len(e.processors)+1)

		for i, p := range e.processors {
			// Retrieve data from the processor.
			m, ok := <-p.C()
			if !ok {
				break loop
			}

			// Set values on returned row.
			for k, v := range m {
				values[0] = k / int64(time.Microsecond) // TODO: Set once per row.
				values[i+1] = v
			}
		}

		// Remove timestamp if there is no group by interval.
		if e.interval == 0 {
			values = values[1:]
		}
		row.Values = append(row.Values, values)
	}

	// Send row to the channel.
	out <- row

	// Mark the end of the output channel.
	close(out)
}

// mapper represents an object for processing iterators.
type mapper struct {
	executor *Executor // parent executor
	seriesID uint32    // series id
	fieldID  uint8     // field id
	typ      DataType  // field data type
	itr      Iterator  // series iterator
	min, max int64     // time range
	interval int64     // group by interval
	fn       mapFunc   // map function

	c    chan map[int64]interface{}
	done chan chan struct{}
}

// newMapper returns a new instance of mapper.
func newMapper(e *Executor, seriesID uint32, fieldID uint8, typ DataType) *mapper {
	return &mapper{
		executor: e,
		seriesID: seriesID,
		fieldID:  fieldID,
		typ:      typ,
		c:        make(chan map[int64]interface{}, 0),
		done:     make(chan chan struct{}, 0),
	}
}

// start begins processing the iterator.
func (m *mapper) start() {
	m.itr = m.executor.db.CreateIterator(m.seriesID, m.fieldID, m.typ,
		m.executor.min, m.executor.max, m.executor.interval)
	go m.run()
}

// stop stops the mapper.
func (m *mapper) stop() { syncClose(m.done) }

// C returns the streaming data channel.
func (m *mapper) C() <-chan map[int64]interface{} { return m.c }

// run executes the map function against the iterator.
func (m *mapper) run() {
	for m.itr.NextIterval() {
		m.fn(m.itr, m)
	}
	close(m.c)
}

// emit sends a value to the reducer's output channel.
func (m *mapper) emit(key int64, value interface{}) {
	m.c <- map[int64]interface{}{key: value}
}

// mapFunc represents a function used for mapping iterators.
type mapFunc func(Iterator, *mapper)

// mapCount computes the number of values in an iterator.
func mapCount(itr Iterator, m *mapper) {
	n := 0
	for k, _ := itr.Next(); k != 0; k, _ = itr.Next() {
		n++
	}
	m.emit(itr.Time(), float64(n))
}

// mapSum computes the summation of values in an iterator.
func mapSum(itr Iterator, m *mapper) {
	n := float64(0)
	for k, v := itr.Next(); k != 0; k, v = itr.Next() {
		n += v.(float64)
	}
	m.emit(itr.Time(), n)
}

// reducer represents an object for processing mapper output.
// Implements processor.
type reducer struct {
	executor *Executor        // parent executor
	stmt     *SelectStatement // substatement
	mappers  []*mapper        // child mappers
	fn       reduceFunc       // reduce function

	c    chan map[int64]interface{}
	done chan chan struct{}
}

// newReducer returns a new instance of reducer.
func newReducer(e *Executor) *reducer {
	return &reducer{
		executor: e,
		c:        make(chan map[int64]interface{}, 0),
		done:     make(chan chan struct{}, 0),
	}
}

// start begins streaming values from the mappers and reducing them.
func (r *reducer) start() {
	for _, m := range r.mappers {
		m.start()
	}
	go r.run()
}

// stop stops the reducer.
func (r *reducer) stop() {
	for _, m := range r.mappers {
		m.stop()
	}
	syncClose(r.done)
}

// C returns the streaming data channel.
func (r *reducer) C() <-chan map[int64]interface{} { return r.c }

// name returns the source name.
func (r *reducer) name() string { return r.stmt.Source.(*Series).Name }

// run runs the reducer loop to read mapper output and reduce it.
func (r *reducer) run() {
loop:
	for {
		// Combine all data from the mappers.
		data := make(map[int64][]interface{})
		for _, m := range r.mappers {
			kv, ok := <-m.C()
			if !ok {
				break loop
			}
			for k, v := range kv {
				data[k] = append(data[k], v)
			}
		}

		// Reduce each key.
		for k, v := range data {
			r.fn(k, v, r)
		}
	}

	// Mark the channel as complete.
	close(r.c)
}

// emit sends a value to the reducer's output channel.
func (r *reducer) emit(key int64, value interface{}) {
	r.c <- map[int64]interface{}{key: value}
}

// reduceFunc represents a function used for reducing mapper output.
type reduceFunc func(int64, []interface{}, *reducer)

// reduceSum computes the sum of values for each key.
func reduceSum(key int64, values []interface{}, r *reducer) {
	var n float64
	for _, v := range values {
		n += v.(float64)
	}
	r.emit(key, n)
}

// processor represents an object for joining reducer output.
type processor interface {
	start()
	stop()
	name() string
	C() <-chan map[int64]interface{}
}

// literalProcessor represents a processor that continually sends a literal value.
type literalProcessor struct {
	val  interface{}
	c    chan map[int64]interface{}
	done chan chan struct{}
}

// newLiteralProcessor returns a literalProcessor for a given value.
func newLiteralProcessor(val interface{}) *literalProcessor {
	return &literalProcessor{
		val:  val,
		c:    make(chan map[int64]interface{}, 0),
		done: make(chan chan struct{}, 0),
	}
}

// C returns the streaming data channel.
func (p *literalProcessor) C() <-chan map[int64]interface{} { return p.c }

// process continually returns a literal value with a "0" key.
func (p *literalProcessor) start() { go p.run() }

// run executes the processor loop.
func (p *literalProcessor) run() {
	for {
		select {
		case ch := <-p.done:
			close(ch)
			return
		case p.c <- map[int64]interface{}{0: p.val}:
		}
	}
}

// stop stops the processor from sending values.
func (p *literalProcessor) stop() { syncClose(p.done) }

// name returns the source name.
func (p *literalProcessor) name() string { return "" }

// syncClose closes a "done" channel and waits for a response.
func syncClose(done chan chan struct{}) {
	ch := make(chan struct{}, 0)
	done <- ch
	<-ch
}

// Iterator represents a forward-only iterator over a set of points.
// The iterator groups points together in interval sets.
type Iterator interface {
	// Next returns the next value from the iterator.
	Next() (key int64, value interface{})

	// NextIterval moves to the next iterval. Returns true unless EOF.
	NextIterval() bool

	// Time returns start time of the current interval.
	Time() int64

	// Interval returns the group by duration.
	Interval() time.Duration
}

// Row represents a single row returned from the execution of a statement.
type Row struct {
	Name    string            `json:"name,omitempty"`
	Tags    map[string]string `json:"tags,omitempty"`
	Columns []string          `json:"columns"`
	Values  [][]interface{}   `json:"values,omitempty"`
	Err     error             `json:"err,omitempty"`
}

// TODO: Walk field expressions to extract subqueries.
// TODO: Resolve subqueries to series ids.
// TODO: send query with all ids to executor (knows to run locally or remote server)
// TODO: executor creates mapper for each series id.
// TODO: Create
