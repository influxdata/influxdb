package influxql

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math"
	"sort"
	"strings"
	"time"
)

// DB represents an interface for creating transactions.
type DB interface {
	Begin() (Tx, error)
}

// Tx represents a transaction.
// The Tx must be opened before being used.
type Tx interface {
	// Opens and closes the transaction.
	Open() error
	Close() error

	// SetNow sets the current time to be used throughout the transaction.
	SetNow(time.Time)

	// Creates a list of iterators for a simple select statement.
	//
	// The statement must adhere to the following rules:
	//   1. It can only have a single VarRef field.
	//   2. It can only have a single source measurement.
	CreateIterators(*SelectStatement) ([]Iterator, error)
}

// Iterator represents a forward-only iterator over a set of points.
type Iterator interface {
	// Tags returns the encoded dimensional tag values.
	Tags() string

	// Next returns the next value from the iterator.
	Next() (key int64, value interface{})
}

// Planner represents an object for creating execution plans.
type Planner struct {
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

// Plan creates an execution plan for the given SelectStatement and returns an Executor.
func (p *Planner) Plan(stmt *SelectStatement) (*Executor, error) {
	now := p.Now()

	// Clone the statement to be planned.
	// Replace instances of "now()" with the current time.
	stmt = stmt.Clone()
	stmt.Condition = Reduce(stmt.Condition, &nowValuer{Now: now})

	// Begin an unopened transaction.
	tx, err := p.DB.Begin()
	if err != nil {
		return nil, err
	}

	// Create the executor.
	e := newExecutor(tx, stmt)

	// Determine group by tag keys.
	interval, tags, err := stmt.Dimensions.Normalize()
	if err != nil {
		return nil, err
	}
	e.interval = interval
	e.tags = tags

	// Generate a processor for each field.
	e.processors = make([]Processor, len(stmt.Fields))
	for i, f := range stmt.Fields {
		p, err := p.planField(e, f)
		if err != nil {
			return nil, err
		}
		e.processors[i] = p
	}

	return e, nil
}

func (p *Planner) planField(e *Executor, f *Field) (Processor, error) {
	return p.planExpr(e, f.Expr)
}

func (p *Planner) planExpr(e *Executor, expr Expr) (Processor, error) {
	switch expr := expr.(type) {
	case *VarRef:
		return p.planRawQuery(e, expr)
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
func (p *Planner) planRawQuery(e *Executor, v *VarRef) (Processor, error) {
	// Convert the statement to a simplified substatement for the single field.
	stmt, err := e.stmt.Substatement(v)
	if err != nil {
		return nil, err
	}

	// Retrieve a list of iterators for the substatement.
	itrs, err := e.tx.CreateIterators(stmt)
	if err != nil {
		return nil, err
	}

	// Create mapper and reducer.
	mappers := make([]*Mapper, len(itrs))
	for i, itr := range itrs {
		mappers[i] = NewMapper(MapRawQuery, itr, e.interval)
	}
	r := NewReducer(ReduceRawQuery, mappers)
	r.name = lastIdent(stmt.Source.(*Measurement).Name)

	return r, nil

}

// planCall generates a processor for a function call.
func (p *Planner) planCall(e *Executor, c *Call) (Processor, error) {
	// Ensure there is a single argument.
	if c.Name == "percentile" {
		if len(c.Args) != 2 {
			return nil, fmt.Errorf("expected two arguments for percentile()")
		}
	} else if len(c.Args) != 1 {
		return nil, fmt.Errorf("expected one argument for %s()", c.Name)
	}

	// Ensure the argument is a variable reference.
	ref, ok := c.Args[0].(*VarRef)
	if !ok {
		return nil, fmt.Errorf("expected field argument in %s()", c.Name)
	}

	// Convert the statement to a simplified substatement for the single field.
	stmt, err := e.stmt.Substatement(ref)
	if err != nil {
		return nil, err
	}

	// Retrieve a list of iterators for the substatement.
	itrs, err := e.tx.CreateIterators(stmt)
	if err != nil {
		return nil, err
	}

	// Retrieve map & reduce functions by name.
	var mapFn MapFunc
	var reduceFn ReduceFunc
	switch strings.ToLower(c.Name) {
	case "count":
		mapFn, reduceFn = MapCount, ReduceSum
	case "sum":
		mapFn, reduceFn = MapSum, ReduceSum
	case "mean":
		mapFn, reduceFn = MapMean, ReduceMean
	case "percentile":
		lit, ok := c.Args[1].(*NumberLiteral)
		if !ok {
			return nil, fmt.Errorf("expected float argument in percentile()")
		}
		mapFn, reduceFn = MapEcho, ReducePercentile(lit.Val)
	default:
		return nil, fmt.Errorf("function not found: %q", c.Name)
	}

	// Create mapper and reducer.
	mappers := make([]*Mapper, len(itrs))
	for i, itr := range itrs {
		mappers[i] = NewMapper(mapFn, itr, e.interval)
	}
	r := NewReducer(reduceFn, mappers)
	r.name = lastIdent(stmt.Source.(*Measurement).Name)

	return r, nil
}

// planBinaryExpr generates a processor for a binary expression.
// A binary expression represents a join operator between two processors.
func (p *Planner) planBinaryExpr(e *Executor, expr *BinaryExpr) (Processor, error) {
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
	tx         Tx               // transaction
	stmt       *SelectStatement // original statement
	processors []Processor      // per-field processors
	interval   time.Duration    // group by interval
	tags       []string         // dimensional tag keys
}

// newExecutor returns an executor associated with a transaction and statement.
func newExecutor(tx Tx, stmt *SelectStatement) *Executor {
	return &Executor{
		tx:   tx,
		stmt: stmt,
	}
}

// Execute begins execution of the query and returns a channel to receive rows.
func (e *Executor) Execute() (<-chan *Row, error) {
	// Open transaction.
	if err := e.tx.Open(); err != nil {
		return nil, err
	}

	// Initialize processors.
	for _, p := range e.processors {
		p.Process()
	}

	// Create output channel and stream data in a separate goroutine.
	out := make(chan *Row, 0)
	go e.execute(out)

	return out, nil
}

// execute runs in a separate separate goroutine and streams data from processors.
func (e *Executor) execute(out chan *Row) {
	// Ensure the transaction closes after execution.
	defer e.tx.Close()

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
				// Lookup row values and populate data.
				values := e.createRowValuesIfNotExists(rows, e.processors[0].Name(), k.Timestamp, k.Values)
				values[i+1] = v
			}
		}
	}

	// Normalize rows and values.
	a := make(Rows, 0, len(rows))
	for _, row := range rows {
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
func (e *Executor) createRowValuesIfNotExists(rows map[string]*Row, name string, timestamp int64, tagset string) []interface{} {
	// TODO: Add "name" to lookup key.

	// Find row by tagset.
	var row *Row
	if row = rows[tagset]; row == nil {
		row = &Row{Name: name}

		// Create tag map.
		row.Tags = make(map[string]string)
		for i, v := range UnmarshalStrings([]byte(tagset)) {
			row.Tags[e.tags[i]] = v
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
		rows[tagset] = row
	}

	// If no values exist or last value doesn't match the timestamp then create new.
	if len(row.Values) == 0 || row.Values[len(row.Values)-1][0] != timestamp {
		values := make([]interface{}, len(e.processors)+1)
		values[0] = timestamp
		row.Values = append(row.Values, values)
	}

	return row.Values[len(row.Values)-1]
}

// Mapper represents an object for processing iterators.
type Mapper struct {
	fn       MapFunc  // map function
	itr      Iterator // iterators
	interval int64    // grouping interval
}

// NewMapper returns a new instance of Mapper with a given function and interval.
func NewMapper(fn MapFunc, itr Iterator, interval time.Duration) *Mapper {
	return &Mapper{
		fn:       fn,
		itr:      itr,
		interval: interval.Nanoseconds(),
	}
}

// Map executes the mapper's function against the iterator.
// Returns a nil emitter if no data was found.
func (m *Mapper) Map() *Emitter {
	e := NewEmitter(1)
	go m.run(e)
	return e
}

func (m *Mapper) run(e *Emitter) {
	// Close emitter when we're done.
	defer func() { _ = e.Close() }()

	// Wrap iterator with buffer.
	bufItr := &bufIterator{itr: m.itr}

	// Determine the start time.
	var tmin int64
	if m.interval > 0 {
		// Align start time to interval.
		tmin, _ = bufItr.Peek()
		tmin -= (tmin % m.interval)
	}

	for {
		// Set the upper bound of the interval.
		if m.interval > 0 {
			bufItr.tmax = tmin + m.interval - 1
		}

		// Execute the map function.
		m.fn(bufItr, e, tmin)

		// Exit if there was only one interval or no more data is available.
		if bufItr.EOF() {
			break
		}

		// Move the interval forward.
		tmin += m.interval
	}
}

// bufIterator represents a buffer iterator.
type bufIterator struct {
	itr  Iterator // underlying iterator
	tmax int64    // maximum key

	buf struct {
		key   int64
		value interface{}
	}
	buffered bool
}

// Tags returns the encoded dimensional values for the iterator.
func (i *bufIterator) Tags() string { return i.itr.Tags() }

// Next returns the next key/value pair from the iterator.
func (i *bufIterator) Next() (key int64, value interface{}) {
	// Read the key/value pair off the buffer or underlying iterator.
	if i.buffered {
		i.buffered = false
	} else {
		i.buf.key, i.buf.value = i.itr.Next()
	}
	key, value = i.buf.key, i.buf.value

	// If key is greater than tmax then put it back on the buffer.
	if i.tmax != 0 && key > i.tmax {
		i.buffered = true
		return 0, nil
	}

	return key, value
}

// Peek returns the next key/value pair but does not move the iterator forward.
func (i *bufIterator) Peek() (key int64, value interface{}) {
	key, value = i.Next()
	i.buffered = true
	return
}

// EOF returns true if there is no more data in the underlying iterator.
func (i *bufIterator) EOF() bool { i.Peek(); return i.buf.key == 0 }

// MapFunc represents a function used for mapping iterators.
type MapFunc func(Iterator, *Emitter, int64)

// MapCount computes the number of values in an iterator.
func MapCount(itr Iterator, e *Emitter, tmin int64) {
	n := 0
	for k, _ := itr.Next(); k != 0; k, _ = itr.Next() {
		n++
	}
	e.Emit(Key{tmin, itr.Tags()}, float64(n))
}

// MapSum computes the summation of values in an iterator.
func MapSum(itr Iterator, e *Emitter, tmin int64) {
	n := float64(0)
	for k, v := itr.Next(); k != 0; k, v = itr.Next() {
		n += v.(float64)
	}
	e.Emit(Key{tmin, itr.Tags()}, n)
}

// Processor represents an object for joining reducer output.
type Processor interface {
	Process()
	Name() string
	C() <-chan map[Key]interface{}
}

// Reducer represents an object for processing mapper output.
// Implements processor.
type Reducer struct {
	name    string
	fn      ReduceFunc // reduce function
	mappers []*Mapper  // child mappers

	c <-chan map[Key]interface{}
}

// NewReducer returns a new instance of reducer.
func NewReducer(fn ReduceFunc, mappers []*Mapper) *Reducer {
	return &Reducer{
		fn:      fn,
		mappers: mappers,
	}
}

// C returns the output channel.
func (r *Reducer) C() <-chan map[Key]interface{} { return r.c }

// Name returns the source name.
func (r *Reducer) Name() string { return r.name }

// Process processes the Reducer.
func (r *Reducer) Process() { r.Reduce() }

// Reduce executes the reducer's function against all output from the mappers.
func (r *Reducer) Reduce() *Emitter {
	inputs := make([]<-chan map[Key]interface{}, len(r.mappers))
	for i, m := range r.mappers {
		inputs[i] = m.Map().C()
	}

	e := NewEmitter(1)
	r.c = e.C()
	go r.run(e, inputs)
	return e
}

func (r *Reducer) run(e *Emitter, inputs []<-chan map[Key]interface{}) {
	// Close emitter when we're done.
	defer func() { _ = e.Close() }()

	// Buffer all the inputs.
	bufInputs := make([]*bufInput, len(inputs))
	for i, input := range inputs {
		bufInputs[i] = &bufInput{c: input}
	}

	// Stream data from the inputs and reduce.
	for {
		// Read all data from the inputers with the same timestamp.
		timestamp := int64(0)
		for _, bufInput := range bufInputs {
			rec := bufInput.peek()
			if rec == nil {
				continue
			}
			if timestamp == 0 || rec.Key.Timestamp < timestamp {
				timestamp = rec.Key.Timestamp
			}
		}

		data := make(map[Key][]interface{})
		for _, bufInput := range bufInputs {
			for {
				rec := bufInput.read()
				if rec == nil {
					break
				}

				if rec.Key.Timestamp != timestamp {
					bufInput.unread(rec)
					break
				}

				data[rec.Key] = append(data[rec.Key], rec.Value)
			}
		}

		if len(data) == 0 {
			break
		}

		// Sort keys.
		keys := make(keySlice, 0, len(data))
		for k := range data {
			keys = append(keys, k)
		}
		sort.Sort(keys)

		// Reduce each key.
		for _, k := range keys {
			r.fn(k, data[k], e)
		}
	}
}

type bufInput struct {
	buf *Record
	c   <-chan map[Key]interface{}
}

func (i *bufInput) read() *Record {
	if i.buf != nil {
		rec := i.buf
		i.buf = nil
		return rec
	}

	m, _ := <-i.c
	return mapToRecord(m)
}

func (i *bufInput) unread(rec *Record) { i.buf = rec }

func (i *bufInput) peek() *Record {
	rec := i.read()
	i.unread(rec)
	return rec
}

type Record struct {
	Key   Key
	Value interface{}
}

func mapToRecord(m map[Key]interface{}) *Record {
	for k, v := range m {
		return &Record{k, v}
	}
	return nil
}

// ReduceFunc represents a function used for reducing mapper output.
type ReduceFunc func(Key, []interface{}, *Emitter)

// ReduceSum computes the sum of values for each key.
func ReduceSum(key Key, values []interface{}, e *Emitter) {
	var n float64
	for _, v := range values {
		n += v.(float64)
	}
	e.Emit(key, n)
}

// MapMean computes the count and sum of values in an iterator to be combined by the reducer.
func MapMean(itr Iterator, e *Emitter, tmin int64) {
	out := &meanMapOutput{}

	for k, v := itr.Next(); k != 0; k, v = itr.Next() {
		out.Count++
		out.Sum += v.(float64)
	}
	e.Emit(Key{tmin, itr.Tags()}, out)
}

type meanMapOutput struct {
	Count int
	Sum   float64
}

// ReduceMean computes the mean of values for each key.
func ReduceMean(key Key, values []interface{}, e *Emitter) {
	out := &meanMapOutput{}
	for _, v := range values {
		val := v.(*meanMapOutput)
		out.Count += val.Count
		out.Sum += val.Sum
	}
	e.Emit(key, out.Sum/float64(out.Count))
}

// MapEcho emits the data points for each group by interval
func MapEcho(itr Iterator, e *Emitter, tmin int64) {
	var values []interface{}

	for k, v := itr.Next(); k != 0; k, v = itr.Next() {
		values = append(values, v)
	}
	e.Emit(Key{tmin, itr.Tags()}, values)
}

// ReducePercentile computes the percentile of values for each key.
func ReducePercentile(percentile float64) ReduceFunc {
	return func(key Key, values []interface{}, e *Emitter) {
		var allValues []float64

		for _, v := range values {
			vals := v.([]interface{})
			for _, v := range vals {
				allValues = append(allValues, v.(float64))
			}
		}

		sort.Float64s(allValues)
		length := len(allValues)
		index := int(math.Floor(float64(length)*percentile/100.0+0.5)) - 1

		if index < 0 || index >= len(allValues) {
			e.Emit(key, 0.0)
		}

		e.Emit(key, allValues[index])
	}
}

func MapRawQuery(itr Iterator, e *Emitter, tmin int64) {
	for k, v := itr.Next(); k != 0; k, v = itr.Next() {
		e.Emit(Key{k, itr.Tags()}, v)
	}
}

type rawQueryMapOutput struct {
	timestamp int64
	value     interface{}
}

func ReduceRawQuery(key Key, values []interface{}, e *Emitter) {
	for _, v := range values {
		e.Emit(key, v)
	}
}

// binaryExprEvaluator represents a processor for combining two processors.
type binaryExprEvaluator struct {
	executor *Executor // parent executor
	lhs, rhs Processor // processors
	op       Token     // operation

	c chan map[Key]interface{}
}

// newBinaryExprEvaluator returns a new instance of binaryExprEvaluator.
func newBinaryExprEvaluator(e *Executor, op Token, lhs, rhs Processor) *binaryExprEvaluator {
	return &binaryExprEvaluator{
		executor: e,
		op:       op,
		lhs:      lhs,
		rhs:      rhs,
		c:        make(chan map[Key]interface{}, 0),
	}
}

// Process begins streaming values from the lhs/rhs processors
func (e *binaryExprEvaluator) Process() {
	e.lhs.Process()
	e.rhs.Process()
	go e.run()
}

// C returns the streaming data channel.
func (e *binaryExprEvaluator) C() <-chan map[Key]interface{} { return e.c }

// name returns the source name.
func (e *binaryExprEvaluator) Name() string { return "" }

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
		m := make(map[Key]interface{})
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
	c    chan map[Key]interface{}
	done chan chan struct{}
}

// newLiteralProcessor returns a literalProcessor for a given value.
func newLiteralProcessor(val interface{}) *literalProcessor {
	return &literalProcessor{
		val:  val,
		c:    make(chan map[Key]interface{}, 0),
		done: make(chan chan struct{}, 0),
	}
}

// C returns the streaming data channel.
func (p *literalProcessor) C() <-chan map[Key]interface{} { return p.c }

// Process continually returns a literal value with a "0" key.
func (p *literalProcessor) Process() { go p.run() }

// run executes the processor loop.
func (p *literalProcessor) run() {
	for {
		select {
		case ch := <-p.done:
			close(ch)
			return
		case p.c <- map[Key]interface{}{Key{}: p.val}:
		}
	}
}

// stop stops the processor from sending values.
func (p *literalProcessor) stop() { syncClose(p.done) }

// name returns the source name.
func (p *literalProcessor) Name() string { return "" }

// syncClose closes a "done" channel and waits for a response.
func syncClose(done chan chan struct{}) {
	ch := make(chan struct{}, 0)
	done <- ch
	<-ch
}

// Key represents a key returned by a Mapper or Reducer.
type Key struct {
	Timestamp int64
	Values    string
}

type keySlice []Key

func (p keySlice) Len() int { return len(p) }
func (p keySlice) Less(i, j int) bool {
	return p[i].Timestamp < p[j].Timestamp || p[i].Values < p[j].Values
}
func (p keySlice) Swap(i, j int) { p[i], p[j] = p[j], p[i] }

// Emitter provides bufferred emit/flush of key/value pairs.
type Emitter struct {
	c chan map[Key]interface{}
}

// NewEmitter returns a new instance of Emitter with a buffer size of n.
func NewEmitter(n int) *Emitter {
	return &Emitter{
		c: make(chan map[Key]interface{}, n),
	}
}

// Close closes the emitter's output channel.
func (e *Emitter) Close() error { close(e.c); return nil }

// C returns the emitter's output channel.
func (e *Emitter) C() <-chan map[Key]interface{} { return e.c }

// Emit sets a key and value on the emitter's bufferred data.
func (e *Emitter) Emit(key Key, value interface{}) { e.c <- map[Key]interface{}{key: value} }

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

// MarshalStrings encodes an array of strings into a byte slice.
func MarshalStrings(a []string) (ret []byte) {
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

// UnmarshalStrings decodes a byte slice into an array of strings.
func UnmarshalStrings(b []byte) (ret []string) {
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
