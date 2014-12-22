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

// DB represents an interface to the underlying storage.
type DB interface {
	// Returns a list of series data ids matching a name and tags.
	MatchSeries(name string, tags map[string]string) []uint32

	// Returns a slice of tag values for a series.
	SeriesTagValues(seriesID uint32, keys []string) []string

	// Returns the id and data type for a series field.
	// Returns id of zero if not a field.
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
		return nil, fmt.Errorf("invalid time range: %s - %s", min.Format(DateTimeFormat), max.Format(DateTimeFormat))
	}
	e.min, e.max = min, max

	// Determine group by interval.
	interval, tags, err := p.normalizeDimensions(stmt.Dimensions)
	if err != nil {
		return nil, err
	}
	e.interval, e.tags = interval, tags

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
func (p *Planner) normalizeDimensions(dimensions Dimensions) (time.Duration, []string, error) {
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
		return lit.Val, dimensionKeys(dimensions[1:]), nil
	}

	return 0, dimensionKeys(dimensions), nil
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
	name := sub.Source.(*Measurement).Name

	// Extract tags from conditional.
	tags := make(map[string]string)
	condition, err := p.extractTags(name, sub.Condition, tags)
	if err != nil {
		return nil, err
	}
	sub.Condition = condition

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
		m.key = append(make([]byte, 8), marshalStrings(p.DB.SeriesTagValues(seriesID, e.tags))...)
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

// extractTags extracts a tag key/value map from a statement.
// Extracted tags are removed from the statement.
func (p *Planner) extractTags(name string, expr Expr, tags map[string]string) (Expr, error) {
	// TODO: Refactor into a walk-like Replace().
	switch expr := expr.(type) {
	case *BinaryExpr:
		// If the LHS is a variable ref then check for tag equality.
		if lhs, ok := expr.LHS.(*VarRef); ok && expr.Op == EQ {
			return p.extractBinaryExprTags(name, expr, lhs, expr.RHS, tags)
		}

		// If the RHS is a variable ref then check for tag equality.
		if rhs, ok := expr.RHS.(*VarRef); ok && expr.Op == EQ {
			return p.extractBinaryExprTags(name, expr, rhs, expr.LHS, tags)
		}

		// Recursively process LHS.
		lhs, err := p.extractTags(name, expr.LHS, tags)
		if err != nil {
			return nil, err
		}
		expr.LHS = lhs

		// Recursively process RHS.
		rhs, err := p.extractTags(name, expr.RHS, tags)
		if err != nil {
			return nil, err
		}
		expr.RHS = rhs

		return expr, nil

	case *ParenExpr:
		e, err := p.extractTags(name, expr.Expr, tags)
		if err != nil {
			return nil, err
		}
		expr.Expr = e
		return expr, nil

	default:
		return expr, nil
	}
}

// extractBinaryExprTags extracts a tag key/value map from a statement.
func (p *Planner) extractBinaryExprTags(name string, expr Expr, ref *VarRef, value Expr, tags map[string]string) (Expr, error) {
	// Ignore if the value is not a string literal.
	lit, ok := value.(*StringLiteral)
	if !ok {
		return expr, nil
	}

	// Extract the key and remove the measurement prefix.
	key := strings.TrimPrefix(ref.Val, name+".")

	// If tag is already filtered then return error.
	if _, ok := tags[key]; ok {
		return nil, fmt.Errorf("duplicate tag filter: %s.%s", name, key)
	}

	// Add tag to the filter.
	tags[key] = lit.Val

	// Return nil to remove the expression.
	return nil, nil
}

// Executor represents the implementation of Executor.
// It executes all reducers and combines their result into a row.
type Executor struct {
	db         DB               // source database
	stmt       *SelectStatement // original statement
	processors []processor      // per-field processors
	min, max   time.Time        // time range
	interval   time.Duration    // group by duration
	tags       []string         // group by tag keys
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
	executor *Executor // parent executor
	seriesID uint32    // series id
	fieldID  uint8     // field id
	typ      DataType  // field data type
	itr      Iterator  // series iterator
	min, max int64     // time range
	interval int64     // group by interval
	key      []byte    // encoded timestamp + dimensional values
	fn       mapFunc   // map function

	c    chan map[string]interface{}
	done chan chan struct{}
}

// newMapper returns a new instance of mapper.
func newMapper(e *Executor, seriesID uint32, fieldID uint8, typ DataType) *mapper {
	return &mapper{
		executor: e,
		seriesID: seriesID,
		fieldID:  fieldID,
		typ:      typ,
		c:        make(chan map[string]interface{}, 0),
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
func (m *mapper) C() <-chan map[string]interface{} { return m.c }

// run executes the map function against the iterator.
func (m *mapper) run() {
	for m.itr.NextIterval() {
		m.fn(m.itr, m)
	}
	close(m.c)
}

// emit sends a value to the mapper's output channel.
func (m *mapper) emit(key int64, value interface{}) {
	// Encode the timestamp to the beginning of the key.
	binary.BigEndian.PutUint64(m.key, uint64(key))

	// OPTIMIZE: Collect emit calls and flush all at once.
	m.c <- map[string]interface{}{string(m.key): value}
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
	executor *Executor        // parent executor
	stmt     *SelectStatement // substatement
	mappers  []*mapper        // child mappers
	fn       reduceFunc       // reduce function

	c    chan map[string]interface{}
	done chan chan struct{}
}

// newReducer returns a new instance of reducer.
func newReducer(e *Executor) *reducer {
	return &reducer{
		executor: e,
		c:        make(chan map[string]interface{}, 0),
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
func (r *reducer) C() <-chan map[string]interface{} { return r.c }

// name returns the source name.
func (r *reducer) name() string { return r.stmt.Source.(*Measurement).Name }

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
