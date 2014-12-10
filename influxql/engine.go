package influxql

import (
	"fmt"
	"strings"
	"time"
)

// Plan generates an executable plan for a SELECT statement.

// list series ->
/*
[
	{
		"name": "cpu",
		"columns": ["id", "region", "host"],
		"values": [
			1, "uswest", "servera",
			2, "uswest", "serverb"
		]
	},
	{
		""
	}
]

list series where region = 'uswest'

list tags where name = 'cpu'

list tagKeys where name = 'cpu'

list series where name = 'cpu' and region = 'uswest'

select distinct(region) from cpu

list names
list tagKeys

list tagValeus where tagKey = 'region' and time > now() -1h

select a.value, b.value from a join b where a.user_id == 100
  select a.value from a where a.user_id == 100
  select b.value from b

          3                1              2
select sum(a.value) + (sum(b.value) / min(b.value)) from a join b group by region

	select suM(a.value) from a group by time(5m)
	select sum(b.value) from b group by time(5m)

execute sum MR on series [23, 65, 88, 99, 101, 232]

map -> 1 tick per 5m
reduce -> combines ticks per 5m interval -> outputs

planner -> take reduce output per 5m interval from the two reducers
           and combine with the join function, which is +

[1,/,2,+,3]



for v := s[0].Next(); v != nil; v = 2[0].Next() {
	var result interface{}
	for i := 1; i < len(s); i += 2 {
		/ it's an operator
		if i % 2 == 1 {

		}
	}
}

select count(distinct(host)) from cpu where time > now() - 5m

type mapper interface {
	Map(iterator)
}

type floatCountMapper struct {}
func(m *floatCountMapper) Map(i Iterator) {
	itr := i.(*floatIterator)
}

type Iterator interface {
	itr()
}

type iterator struct {
	cursor *bolt.Cursor
	timeBucket time.Time
	name string
	seriesID uint32
	tags map[string]string
	fieldID uint8
	where *WhereClause
}

func (i *intIterator) itr() {}
func (i *intIterator) Next() (k int64, v float64) {
	// loop through bolt cursor applying where clause and yield next point
	// if cursor is at end or time is out of range, yield nil
}

*/

type DB interface {
	// Returns a list of series data ids matching a name and tags.
	MatchSeries(name string, tags map[string]string) []uint32

	// Returns the id and data type for a series field.
	Field(name, field string) (fieldID uint8, typ DataType)

	// Returns an iterator given a series data id, field id, & field data type.
	CreateIterator(id uint32, fieldID uint8, typ DataType) Iterator
}

// Planner represents an object for creating execution plans.
type Planner struct {
	DB DB
}

func (p *Planner) Plan(stmt *SelectStatement) (*Executor, error) {
	// Create the executor.
	e := &Executor{
		db:         p.DB,
		stmt:       stmt,
		processors: make([]processor, len(stmt.Fields)),
	}

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
	fname := strings.TrimPrefix(ref.Val, name)
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
		r.mappers[i] = newMapper(e, seriesID, fieldID, typ)
	}

	// Set the appropriate reducer function.
	switch strings.ToLower(c.Name) {
	case "count":
		r.fn = reduceCount
		for _, m := range r.mappers {
			m.fn = mapCount
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
	db         DB
	stmt       *SelectStatement
	processors []processor
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

	// Combine values from each processor.
	row := &Row{}
	row.Values = []map[string]interface{}{
		make(map[string]interface{}),
	}
	for i, p := range e.processors {
		f := e.stmt.Fields[i]

		// Retrieve data from the processor.
		m, ok := <-p.C()
		if !ok {
			continue
		}

		// Set values on returned row.
		row.Name = p.name()
		for k, v := range m {
			if k != 0 {
				row.Values[0]["timestamp"] = time.Unix(0, k).UTC().Format(time.RFC3339Nano)
			}
			row.Values[0][f.Name()] = v
		}
	}

	// Send row to the channel.
	if len(row.Values[0]) != 0 {
		out <- row
	}

	// Mark the end of the output channel.
	close(out)
}

/*
select derivative(mean(value))
from cpu
group by time(5m)

select mean(value) from cpu group by time(5m)
select top(10, value) from cpu group by host where time > now() - 1h

this query uses this type of cycle
-------REMOTE HOST ------------- -----HOST THAT GOT QUERY ---
map -> reduce -> combine -> map  -> reduce -> combine -> user

select mean(value) cpu group by time(5m), host where time > now() -4h
map -> reduce -> combine -> user
map -> reduce -> map -> reduce -> combine -> user
map -> reduce -> combine -> map -> reduce -> combine -> user


select value from
(
	select mean(value) AS value FROM cpu GROUP BY time(5m)
)

[
{
	name: cpu,
	tags: {
		host: servera,
	},
	columns: [time, mean],
	values : [
		[23423423, 88.8]
	]
},
{
	name: cpu,
	tags: {
		host: serverb,
	}
}
]
*/

// mapper represents an object for processing iterators.
type mapper struct {
	executor *Executor // parent executor
	seriesID uint32    // series id
	fieldID  uint8     // field id
	typ      DataType  // field data type
	itr      Iterator  // series iterator
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
	m.itr = m.executor.db.CreateIterator(m.seriesID, m.fieldID, m.typ)
	go m.run()
}

// stop stops the mapper.
func (m *mapper) stop() { syncClose(m.done) }

// C returns the streaming data channel.
func (m *mapper) C() <-chan map[int64]interface{} { return m.c }

// run executes the map function against the iterator.
func (m *mapper) run() {
	m.fn(m.itr, m)
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
	// Combine all data from the mappers.
	data := make(map[int64][]interface{})
	for _, m := range r.mappers {
		kv := <-m.C()
		for k, v := range kv {
			data[k] = append(data[k], v)
		}
	}

	// Reduce each key.
	for k, v := range data {
		r.fn(k, v, r)
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

// reduceCount computes the number of values for each key.
func reduceCount(key int64, values []interface{}, r *reducer) {
	r.emit(key, len(values))
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
type Iterator interface {
	// Next returns the next value from the iterator.
	Next() (key int64, value interface{})

	// Time returns start time of the current interval.
	Time() int64

	// Duration returns the group by duration.
	Duration() time.Duration
}

// Row represents a single row returned from the execution of a statement.
type Row struct {
	Name   string                   `json:"name,omitempty"`
	Tags   map[string]string        `json:"tags,omitempty"`
	Values []map[string]interface{} `json:"values,omitempty"`
	Err    error                    `json:"err,omitempty"`
}

// TODO: Walk field expressions to extract subqueries.
// TODO: Resolve subqueries to series ids.
// TODO: send query with all ids to executor (knows to run locally or remote server)
// TODO: executor creates mapper for each series id.
// TODO: Create
