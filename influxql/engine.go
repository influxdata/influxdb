package influxql

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
	"time"
)

// IteratorCreator represents an interface for creating iterators.
type IteratorCreator interface {
	// Returns an set of iterators to execute over all data for a simple select statement.
	CreateIterators(*SelectStatement) ([]Iterator, error)
}

// Iterator represents a forward-only iterator over a set of points.
// The iterator groups points together in interval sets.
type Iterator interface {
	Open() error
	Close() error

	// Next returns the next value from the iterator.
	Next() (key int64, value interface{})

	// NextIterval moves to the next iterval. Returns true unless EOF.
	NextIterval() bool

	// Key returns the interval start time and tag values encoded as a byte slice.
	Key() []byte
}

// Planner represents an object for creating execution plans.
type Planner struct {
	DB IteratorCreator

	// Returns the current time. Defaults to time.Now().
	Now func() time.Time
}

// NewPlanner returns a new instance of Planner.
func NewPlanner(db IteratorCreator) *Planner {
	return &Planner{
		DB:  db,
		Now: time.Now,
	}
}

func (p *Planner) Plan(stmt *SelectStatement) (*Executor, error) {
	// Retrieve current time so it's consistent throughout.
	now := p.Now()

	// Create the executor.
	e := &Executor{
		db:         p.DB,
		stmt:       stmt,
		processors: make([]processor, len(stmt.Fields)),
	}

	// Reduce and replace current time.
	stmt.Condition = Reduce(stmt.Condition, &nowValuer{Now: now})

	// Require a lower time bound.
	// Add an upper time bound if one does not exist.
	tmin, tmax := TimeRange(stmt.Condition)
	if tmin.IsZero() {
		return nil, errors.New("statement must have a lower time bound")
	} else if tmax.IsZero() {
		// Add 'AND time <= now()' to condition.
		cond := &BinaryExpr{LHS: &VarRef{Val: "time"}, RHS: &TimeLiteral{Val: now}, Op: LTE}
		if stmt.Condition == nil {
			stmt.Condition = cond
		} else {
			stmt.Condition = &BinaryExpr{LHS: stmt.Condition, RHS: cond, Op: AND}
		}
	}

	// Determine group by tag keys.
	_, tagKeys, err := stmt.Dimensions.Normalize()
	if err != nil {
		return nil, err
	}
	e.tagKeys = tagKeys

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

	// TODO: Map filter sets to statements.

	// Retrieve a list of iterators for the substatement.
	iterators, err := p.DB.CreateIterators(sub)
	if err != nil {
		return nil, err
	}

	// Generate a reducer for the given function.
	r := newReducer()
	r.source = sub.Source.(*Measurement).Name

	// Generate mappers for each id.
	r.mappers = make([]*mapper, len(iterators))
	for i, itr := range iterators {
		r.mappers[i] = newMapper(itr)
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
	// Create processor for LHS.
	lhs, err := p.planExpr(e, expr.LHS)
	if err != nil {
		return nil, fmt.Errorf("lhs: %s", err)
	}

	// Create processor for RHS.
	rhs, err := p.planExpr(e, expr.RHS)
	if err != nil {
		return nil, fmt.Errorf("rhs: %s", err)
	}

	// Combine processors.
	return newBinaryExprEvaluator(e, expr.Op, lhs, rhs), nil
}

// Executor represents the implementation of Executor.
// It executes all reducers and combines their result into a row.
type Executor struct {
	db         IteratorCreator  // iterator source
	stmt       *SelectStatement // original statement
	processors []processor      // per-field processors
	tagKeys    []string         // dimensional tag keys
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

	// Initialize map of rows by encoded tagset.
	rows := make(map[string]*Row)

	// Combine values from each processor.
loop:
	for {
		// Retrieve values from processors and write them to the approprite
		// row based on their tagset.
		for i, p := range e.processors {
			// Retrieve data from the processor.
			m, ok := <-p.C()
			if !ok {
				break loop
			}

			// Set values on returned row.
			for k, v := range m {
				// Extract timestamp and tag values from key.
				b := []byte(k)
				timestamp := int64(binary.BigEndian.Uint64(b[0:8]))

				// Lookup row values and populate data.
				values := e.createRowValuesIfNotExists(rows, e.processors[0].name(), b[8:], timestamp)
				values[i+1] = v
			}
		}
	}

	// Normalize rows and values.
	// This converts the timestamps from nanoseconds to microseconds.
	a := make(Rows, 0, len(rows))
	for _, row := range rows {
		for _, values := range row.Values {
			values[0] = values[0].(int64) / int64(time.Microsecond)
		}
		a = append(a, row)
	}
	sort.Sort(a)

	// Send rows to the channel.
	for _, row := range a {
		out <- row
	}

	// Mark the end of the output channel.
	close(out)
}

// creates a new value set if one does not already exist for a given tagset + timestamp.
func (e *Executor) createRowValuesIfNotExists(rows map[string]*Row, name string, tagset []byte, timestamp int64) []interface{} {
	// TODO: Add "name" to lookup key.

	// Find row by tagset.
	var row *Row
	if row = rows[string(tagset)]; row == nil {
		row = &Row{Name: name}

		// Create tag map.
		row.Tags = make(map[string]string)
		for i, v := range unmarshalStrings(tagset) {
			row.Tags[e.tagKeys[i]] = v
		}

		// Create column names.
		row.Columns = make([]string, 1, len(e.stmt.Fields)+1)
		row.Columns[0] = "time"
		for i, f := range e.stmt.Fields {
			name := f.Name()
			if name == "" {
				name = fmt.Sprintf("col%d", i)
			}
			row.Columns = append(row.Columns, name)
		}

		// Save to lookup.
		rows[string(tagset)] = row
	}

	// If no values exist or last value doesn't match the timestamp then create new.
	if len(row.Values) == 0 || row.Values[len(row.Values)-1][0] != timestamp {
		values := make([]interface{}, len(e.processors)+1)
		values[0] = timestamp
		row.Values = append(row.Values, values)
	}

	return row.Values[len(row.Values)-1]
}

// dimensionKeys returns a list of tag key names for the dimensions.
// Each dimension must be a VarRef.
func dimensionKeys(dimensions Dimensions) (a []string) {
	for _, d := range dimensions {
		a = append(a, d.Expr.(*VarRef).Val)
	}
	return
}

// mapper represents an object for processing iterators.
type mapper struct {
	itr Iterator // iterator
	fn  mapFunc  // map function

	c    chan map[string]interface{}
	done chan chan struct{}
}

// newMapper returns a new instance of mapper.
func newMapper(itr Iterator) *mapper {
	return &mapper{
		itr:  itr,
		c:    make(chan map[string]interface{}, 0),
		done: make(chan chan struct{}, 0),
	}
}

// start begins processing the iterator.
func (m *mapper) start() {
	m.itr.Open()
	go m.run()
}

// stop stops the mapper.
func (m *mapper) stop() {
	m.itr.Close()
	syncClose(m.done)
}

// C returns the streaming data channel.
func (m *mapper) C() <-chan map[string]interface{} { return m.c }

// run executes the map function against the iterator.
func (m *mapper) run() {
	for m.itr.NextIterval() {
		m.fn(m.itr, m)
	}
	close(m.c)
}

// emit sends a value to the mapper's output channel.
func (m *mapper) emit(key []byte, value interface{}) {
	// OPTIMIZE: Collect emit calls and flush all at once.
	m.c <- map[string]interface{}{string(key): value}
}

// mapFunc represents a function used for mapping iterators.
type mapFunc func(Iterator, *mapper)

// mapCount computes the number of values in an iterator.
func mapCount(itr Iterator, m *mapper) {
	n := 0
	for k, _ := itr.Next(); k != 0; k, _ = itr.Next() {
		n++
	}
	m.emit(itr.Key(), float64(n))
}

// mapSum computes the summation of values in an iterator.
func mapSum(itr Iterator, m *mapper) {
	n := float64(0)
	for k, v := itr.Next(); k != 0; k, v = itr.Next() {
		n += v.(float64)
	}
	m.emit(itr.Key(), n)
}

// processor represents an object for joining reducer output.
type processor interface {
	start()
	stop()
	name() string
	C() <-chan map[string]interface{}
}

// reducer represents an object for processing mapper output.
// Implements processor.
type reducer struct {
	source  string     // source name
	mappers []*mapper  // child mappers
	fn      reduceFunc // reduce function

	c    chan map[string]interface{}
	done chan chan struct{}
}

// newReducer returns a new instance of reducer.
func newReducer() *reducer {
	return &reducer{
		c:    make(chan map[string]interface{}, 0),
		done: make(chan chan struct{}, 0),
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
func (r *reducer) C() <-chan map[string]interface{} { return r.c }

// name returns the source name.
func (r *reducer) name() string { return r.source }

// run runs the reducer loop to read mapper output and reduce it.
func (r *reducer) run() {
loop:
	for {
		// Combine all data from the mappers.
		data := make(map[string][]interface{})
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
func (r *reducer) emit(key string, value interface{}) {
	r.c <- map[string]interface{}{key: value}
}

// reduceFunc represents a function used for reducing mapper output.
type reduceFunc func(string, []interface{}, *reducer)

// reduceSum computes the sum of values for each key.
func reduceSum(key string, values []interface{}, r *reducer) {
	var n float64
	for _, v := range values {
		n += v.(float64)
	}
	r.emit(key, n)
}

// binaryExprEvaluator represents a processor for combining two processors.
type binaryExprEvaluator struct {
	executor *Executor // parent executor
	lhs, rhs processor // processors
	op       Token     // operation

	c    chan map[string]interface{}
	done chan chan struct{}
}

// newBinaryExprEvaluator returns a new instance of binaryExprEvaluator.
func newBinaryExprEvaluator(e *Executor, op Token, lhs, rhs processor) *binaryExprEvaluator {
	return &binaryExprEvaluator{
		executor: e,
		op:       op,
		lhs:      lhs,
		rhs:      rhs,
		c:        make(chan map[string]interface{}, 0),
		done:     make(chan chan struct{}, 0),
	}
}

// start begins streaming values from the lhs/rhs processors
func (e *binaryExprEvaluator) start() {
	e.lhs.start()
	e.rhs.start()
	go e.run()
}

// stop stops the processor.
func (e *binaryExprEvaluator) stop() {
	e.lhs.stop()
	e.rhs.stop()
	syncClose(e.done)
}

// C returns the streaming data channel.
func (e *binaryExprEvaluator) C() <-chan map[string]interface{} { return e.c }

// name returns the source name.
func (e *binaryExprEvaluator) name() string { return "" }

// run runs the processor loop to read subprocessor output and combine it.
func (e *binaryExprEvaluator) run() {
	for {
		// Read LHS value.
		lhs, ok := <-e.lhs.C()
		if !ok {
			break
		}

		// Read RHS value.
		rhs, ok := <-e.rhs.C()
		if !ok {
			break
		}

		// Merge maps.
		m := make(map[string]interface{})
		for k, v := range lhs {
			m[k] = e.eval(v, rhs[k])
		}
		for k, v := range rhs {
			// Skip value if already processed in lhs loop.
			if _, ok := m[k]; ok {
				continue
			}
			m[k] = e.eval(float64(0), v)
		}

		// Return value.
		e.c <- m
	}

	// Mark the channel as complete.
	close(e.c)
}

// eval evaluates two values using the evaluator's operation.
func (e *binaryExprEvaluator) eval(lhs, rhs interface{}) interface{} {
	switch e.op {
	case ADD:
		return lhs.(float64) + rhs.(float64)
	case SUB:
		return lhs.(float64) - rhs.(float64)
	case MUL:
		return lhs.(float64) * rhs.(float64)
	case DIV:
		rhs := rhs.(float64)
		if rhs == 0 {
			return float64(0)
		}
		return lhs.(float64) / rhs
	default:
		// TODO: Validate operation & data types.
		panic("invalid operation: " + e.op.String())
	}
}

// literalProcessor represents a processor that continually sends a literal value.
type literalProcessor struct {
	val  interface{}
	c    chan map[string]interface{}
	done chan chan struct{}
}

// newLiteralProcessor returns a literalProcessor for a given value.
func newLiteralProcessor(val interface{}) *literalProcessor {
	return &literalProcessor{
		val:  val,
		c:    make(chan map[string]interface{}, 0),
		done: make(chan chan struct{}, 0),
	}
}

// C returns the streaming data channel.
func (p *literalProcessor) C() <-chan map[string]interface{} { return p.c }

// process continually returns a literal value with a "0" key.
func (p *literalProcessor) start() { go p.run() }

// run executes the processor loop.
func (p *literalProcessor) run() {
	for {
		select {
		case ch := <-p.done:
			close(ch)
			return
		case p.c <- map[string]interface{}{"": p.val}:
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

// Row represents a single row returned from the execution of a statement.
type Row struct {
	Name    string            `json:"name,omitempty"`
	Tags    map[string]string `json:"tags,omitempty"`
	Columns []string          `json:"columns"`
	Values  [][]interface{}   `json:"values,omitempty"`
	Err     error             `json:"err,omitempty"`
}

// tagsHash returns a hash of tag key/value pairs.
func (r *Row) tagsHash() uint64 {
	h := fnv.New64a()
	keys := r.tagsKeys()
	for _, k := range keys {
		h.Write([]byte(k))
		h.Write([]byte(r.Tags[k]))
	}
	return h.Sum64()
}

// tagKeys returns a sorted list of tag keys.
func (r *Row) tagsKeys() []string {
	a := make([]string, len(r.Tags))
	for k := range r.Tags {
		a = append(a, k)
	}
	sort.Strings(a)
	return a
}

// Rows represents a list of rows that can be sorted consistently by name/tag.
type Rows []*Row

func (p Rows) Len() int { return len(p) }

func (p Rows) Less(i, j int) bool {
	// Sort by name first.
	if p[i].Name != p[j].Name {
		return p[i].Name < p[j].Name
	}

	// Sort by tag set hash. Tags don't have a meaningful sort order so we
	// just compute a hash and sort by that instead. This allows the tests
	// to receive rows in a predictable order every time.
	return p[i].tagsHash() < p[j].tagsHash()
}

func (p Rows) Swap(i, j int) { p[i], p[j] = p[j], p[i] }

// MarshalKey encodes a timestamp and string values into a byte slice.
func MarshalKey(t int64, a []string) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[0:8], uint64(t))
	return append(b[:], marshalStrings(a)...)
}

// UnmarshalKey decodes a timestamp and string values from a byte slice.
func UnmarshalKey(b []byte) (t int64, a []string) {
	t = int64(binary.BigEndian.Uint64(b))
	a = unmarshalStrings(b[8:])
	return
}

// marshalStrings encodes an array of strings into a byte slice.
func marshalStrings(a []string) (ret []byte) {
	for _, s := range a {
		// Create a slice for len+data
		b := make([]byte, 2+len(s))
		binary.BigEndian.PutUint16(b[0:2], uint16(len(s)))
		copy(b[2:], s)

		// Append it to the full byte slice.
		ret = append(ret, b...)
	}
	return
}

// unmarshalStrings decodes a byte slice into an array of strings.
func unmarshalStrings(b []byte) (ret []string) {
	for {
		// If there's no more data then exit.
		if len(b) == 0 {
			return
		}

		// Decode size + data.
		n := binary.BigEndian.Uint16(b[0:2])
		ret = append(ret, string(b[2:n+2]))

		// Move the byte slice forward and retry.
		b = b[n+2:]
	}
}
