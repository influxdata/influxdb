package influxql

import (
	"bytes"
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
	// Create MapReduceJobs for the given select statement. One MRJob will be created per unique tagset that matches the query
	CreateMapReduceJobs(stmt *influxql.SelectStatement, tagKeys []string, interval int64) ([]*influxql.MapReduceJob, error)
}

// Iterator represents a forward-only iterator over a set of points. These are used by the MapFunctions defined in functions.go
type Iterator interface {
	Next() (seriesID uint16, timestamp int64, value interface{})
	CurrentFieldValues() map[uint8]interface{}
	Tags(seriesID uint16) map[string]string
}

type MapReduceJob struct {
	MeasurementName string
	TagSet          *TagSet
	Reducer         *Reducer
	TMin            int64  // minimum time specified in the query
	TMax            int64  // maximum time specified in the query
	Interval        int64  // group by interval in nanoseconds
	FuncCount       int    // number of map/reduce functions are in the job
	key             []byte // a key that identifies the MRJob so it can be sorted
}

func (m *MapReduceJob) Open() error {
	for _, mm := range m.Reducer.Mappers {
		if err := mm.Open(); err != nil {
			m.Close()
			return err
		}
	}
}

func (m *MapReduceJob) Close() {
	for _, mm := range m.Reducer.Mappers {
		mm.Close()
	}
}

func (m *MapReduceJob) Key() []byte {
	if m.key == nil {
		m.key = append([]byte(m.MeasurementName), m.TagSet.Key...)
	}
	return m.key
}

type funcOutput struct {
	emmitters []*localEmitter
	times     []int64
	values    []interface{}
	closed    bool
}

// pull gets the lates times and values from the mapper and returns true if all emitters are closed
func (m *funcOutput) pull() bool {
	if m.closed {
		return m.closed
	}

	m.closed = true
	for i, e := range m.emmitters {
		td, ok := <-e.out
		if ok {
			m.closed = false
			m.times[i] = td.timestamp
			m.values[i] = td.data
		}
	}

	return m.closed
}

func newFuncOutput(funcCount int) *funcOutput {
	output := &funcOutput{
		emitters: make([]*localEmitter, funcCount),
		times:    make([]int64, funcCount),
		values:   make([][]interface{}, funcCount),
	}
	for i, _ := range output.emmitters {
		output.emmitters[i] = newLocalEmitter()
	}
	return output
}

func (m *MapReduceJob) Execute(out chan *Row) {
	outputs := make([]*mapperOutput, len(m.Reducer.Mappers))

	for i, mm := range m.Reducer.Mappers {
		output := newLocalOutput(m.FuncCount)
		outputs[i] = output
		go mm.Run(output.emmitters)
	}

	out <- m.Reducer.Reduce(outputs)
}

type MapReduceJobs []*MapReduceJob

func (a MapReduceJobs) Len() int           { return len(a) }
func (a MapReduceJobs) Less(i, j int) bool { return bytes.Compare(a[i].Key(), a[j].Key()) == -1 }
func (a MapReduceJobs) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

// Mapper is a collection of map functions. A single mapper will be created
// for each shard for each tagset that must be hit to satisfy a query.
// Mappers can either point to a local shard or could point to a remote server.
type Mapper interface {
	Run(e []Emitter) error
	Open() error
	Close()
}

type Reducer struct {
	Mappers     []Mapper
	ReduceFuncs []ReduceFunc
}

type mapFuncOutput struct {
	values []interface{}
}

func (r *Reducer) Reduce(outputs []*funcOutput) *Row {
	minTime := math.MaxInt64
	for _, o := range outputs {
		o.pull()
		if o.times[0] < minTime {
			minTime = o.times[0]
		}
	}

	for {
		mapFuncOutputs := make([]*mapFuncOutput, len(r.ReduceFuncs))
		for {
			matched = false
			for _, o := range outputs {
				if o.times[0] == tmin {
					values
				}
			}
			if !matched {
				minTime = math.MaxInt64
				for _, o := range outputs {
					if o.times[0] < minTime {
						minTime = o.times[0]
					}
				}
				break
			}
		}
		closed := true
		for _, o := range outputs {
			if !o.pull() {
				closed = false
			}
		}
		if closed {
			return row
		}
	}
}

type TagSet struct {
	Tags      map[string]string
	Filters   []*influxql.Expr
	SeriesIDs []uint16
	Key       []byte
}

func NewTagSet() *TagSet {
	return &TagSet{SeriesFilters: make(map[uint16]*influxql.Expr)}
}

func (t *TagSet) AddFilter(id uint16, filter Expr) {
	t.SeriesIDs = append(t.SeriesIDs, id)
	t.Filters = append(t.Filters, filter)
}

type Emitter interface {
	Emit(timestamp int64, data interface{})
	// Close closes the emitter. If an error is passed in it means that the emitter didn't get to complete before some error in processing was hit
	Close(error) error
}

type timeData struct {
	timestamp int64
	data      interface{}
}

type localEmitter struct {
	out chan *timeData
}

func newLocalEmitter() *localEmitter {
	return &localEmitter{make(chan *timeData, 0)}
}

func (e *localEmitter) Emit(timestamp int64, data interface{}) {
	e.out <- &timeData{timestamp, data}
}

func (e localEmitter) Close(error) error {
	close(e.out)
	return nil
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

	// Determine group by tag keys.
	interval, tags, err := stmt.Dimensions.Normalize()
	if err != nil {
		return nil, err
	}

	// TODO: hanldle queries that select from multiple measurements. This assumes that we're only selecting from a single one
	jobs := tx.CreateMapReduceJobs(stmt, tags, interval)

	return &Executor{tx: tx, stmt: stmt, jobs: jobs}, nil
}

// Executor represents the implementation of Executor.
// It executes all reducers and combines their result into a row.
type Executor struct {
	tx   Tx               // transaction
	stmt *SelectStatement // original statement
	jobs []*MapReduceJob  // one job per unique tag set that will return in the query
}

// Execute begins execution of the query and returns a channel to receive rows.
func (e *Executor) Execute() (<-chan *Row, error) {
	// Open transaction.
	for _, j := range e.jobs {
		if err := j.Open(); err != nil {
			e.close()
			return nil, err
		}
	}

	// Create output channel and stream data in a separate goroutine.
	out := make(chan *Row, 0)
	go e.execute(out)

	return out, nil
}

func (e *Executor) close() {
	for _, m := range e.jobs {
		job.Close()
	}
}

// execute runs in a separate separate goroutine and streams data from processors.
func (e *Executor) execute(out chan *Row) {
	// Ensure the the MRJobs close after execution.
	defer e.close()

	// Execute each MRJob serially
	for _, j := range e.jobs {
		job.Execute(out)
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
