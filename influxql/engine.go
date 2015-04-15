package influxql

import (
	"bytes"
	"errors"
	"hash/fnv"
	"math"
	"sort"
	"time"
)

// DB represents an interface for creating transactions.
type DB interface {
	Begin() (Tx, error)
}

const (
	// Return an error if the user is trying to select more than this number of points in a group by statement.
	// Most likely they specified a group by interval without time boundaries.
	MaxGroupByPoints = 100000

	// Since time is always selected, the column count when selecting only a single other value will be 2
	SelectColumnCountWithOneValue = 2

	// IgnoredChunkSize is what gets passed into Mapper.Begin for aggregate queries as they don't chunk points out
	IgnoredChunkSize = 0
)

// Tx represents a transaction.
// The Tx must be opened before being used.
type Tx interface {
	// Create MapReduceJobs for the given select statement. One MRJob will be created per unique tagset that matches the query
	CreateMapReduceJobs(stmt *SelectStatement, tagKeys []string) ([]*MapReduceJob, error)
}

type MapReduceJob struct {
	MeasurementName string
	TagSet          *TagSet
	Mappers         []Mapper         // the mappers to hit all shards for this MRJob
	TMin            int64            // minimum time specified in the query
	TMax            int64            // maximum time specified in the query
	key             []byte           // a key that identifies the MRJob so it can be sorted
	interval        int64            // the group by interval of the query
	stmt            *SelectStatement // the select statement this job was created for
	chunkSize       int              // the number of points to buffer in raw queries before returning a chunked response
}

func (m *MapReduceJob) Open() error {
	for _, mm := range m.Mappers {
		if err := mm.Open(); err != nil {
			m.Close()
			return err
		}
	}
	return nil
}

func (m *MapReduceJob) Close() {
	for _, mm := range m.Mappers {
		mm.Close()
	}
}

func (m *MapReduceJob) Key() []byte {
	if m.key == nil {
		m.key = append([]byte(m.MeasurementName), m.TagSet.Key...)
	}
	return m.key
}

func (m *MapReduceJob) Execute(out chan *Row, filterEmptyResults bool) {
	if err := m.Open(); err != nil {
		out <- &Row{Err: err}
		m.Close()
		return
	}
	defer m.Close()

	// if it's a raw query we handle processing differently
	if m.stmt.IsRawQuery {
		m.processRawQuery(out, filterEmptyResults)
		return
	}

	// get the aggregates and the associated reduce functions
	aggregates := m.stmt.FunctionCalls()
	reduceFuncs := make([]ReduceFunc, len(aggregates))
	for i, c := range aggregates {
		reduceFunc, err := InitializeReduceFunc(c)
		if err != nil {
			out <- &Row{Err: err}
			return
		}
		reduceFuncs[i] = reduceFunc
	}

	// we'll have a fixed number of points with timestamps in buckets. Initialize those times and a slice to hold the associated values
	var pointCountInResult int

	// if the user didn't specify a start time or a group by interval, we're returning a single point that describes the entire range
	if m.TMin == 0 || m.interval == 0 {
		// they want a single aggregate point for the entire time range
		m.interval = m.TMax - m.TMin
		pointCountInResult = 1
	} else {
		intervalTop := m.TMax/m.interval*m.interval + m.interval
		intervalBottom := m.TMin / m.interval * m.interval
		pointCountInResult = int((intervalTop - intervalBottom) / m.interval)
	}

	// For group by time queries, limit the number of data points returned by the limit and offset
	// raw query limits are handled elsewhere
	if m.stmt.Limit > 0 || m.stmt.Offset > 0 {
		// ensure that the offset isn't higher than the number of points we'd get
		if m.stmt.Offset > pointCountInResult {
			return
		}

		// take the lesser of either the pre computed number of group by buckets that
		// will be in the result or the limit passed in by the user
		if m.stmt.Limit < pointCountInResult {
			pointCountInResult = m.stmt.Limit
		}
	}

	// If we are exceeding our MaxGroupByPoints and we aren't a raw query, error out
	if pointCountInResult > MaxGroupByPoints {
		out <- &Row{
			Err: errors.New("too many points in the group by interval. maybe you forgot to specify a where time clause?"),
		}
		return
	}

	// initialize the times of the aggregate points
	resultValues := make([][]interface{}, pointCountInResult)

	// ensure that the start time for the results is on the start of the window
	startTimeBucket := m.TMin / m.interval * m.interval

	for i, _ := range resultValues {
		var t int64
		if m.stmt.Offset > 0 {
			t = startTimeBucket + (int64(i+1) * m.interval * int64(m.stmt.Offset))
		} else {
			t = startTimeBucket + (int64(i+1) * m.interval) - m.interval
		}

		// If we start getting out of our max time range, then truncate values and return
		if t > m.TMax {
			resultValues = resultValues[:i]
			break
		}

		// we always include time so we need one more column than we have aggregates
		vals := make([]interface{}, 0, len(aggregates)+1)
		resultValues[i] = append(vals, time.Unix(0, t).UTC())
	}

	// This just makes sure that if they specify a start time less than what the start time would be with the offset,
	// we just reset the start time to the later time to avoid going over data that won't show up in the result.
	if m.stmt.Offset > 0 {
		m.TMin = resultValues[0][0].(time.Time).UnixNano()
	}

	// now loop through the aggregate functions and populate everything
	for i, c := range aggregates {
		if err := m.processAggregate(c, reduceFuncs[i], resultValues); err != nil {
			out <- &Row{
				Name: m.MeasurementName,
				Tags: m.TagSet.Tags,
				Err:  err,
			}

			return
		}
	}

	// filter out empty results
	if filterEmptyResults && m.resultsEmpty(resultValues) {
		return
	}

	// put together the row to return
	columnNames := make([]string, len(m.stmt.Fields)+1)
	columnNames[0] = "time"
	for i, f := range m.stmt.Fields {
		columnNames[i+1] = f.Name()
	}

	// processes the result values if there's any math in there
	resultValues = m.processResults(resultValues)

	// handle any fill options
	resultValues = m.processFill(resultValues)

	row := &Row{
		Name:    m.MeasurementName,
		Tags:    m.TagSet.Tags,
		Columns: columnNames,
		Values:  resultValues,
	}

	// and we out
	out <- row
}

// processRawQuery will handle running the mappers and then reducing their output
// for queries that pull back raw data values without computing any kind of aggregates.
func (m *MapReduceJob) processRawQuery(out chan *Row, filterEmptyResults bool) {
	// initialize the mappers
	for _, mm := range m.Mappers {
		if err := mm.Begin(nil, m.TMin, m.chunkSize); err != nil {
			out <- &Row{Err: err}
			return
		}
	}

	mapperOutputs := make([][]*rawQueryMapOutput, len(m.Mappers))
	// markers for which mappers have been completely emptied
	mapperComplete := make([]bool, len(m.Mappers))

	// for limit and offset we need to track how many values we've swalloed for the offset and how many we've already set for the limit.
	// we track the number set for the limit because they could be getting chunks. For instance if your limit is 10k, but chunk size is 1k
	valuesSent := 0
	valuesOffset := 0
	valuesToReturn := make([]*rawQueryMapOutput, 0)

	// loop until we've emptied out all the mappers and sent everything out
	for {
		// collect up to the limit for each mapper
		for j, mm := range m.Mappers {
			// only pull from mappers that potentially have more data and whose last output has been completely sent out.
			if mapperOutputs[j] != nil || mapperComplete[j] {
				continue
			}

			res, err := mm.NextInterval()
			if err != nil {
				out <- &Row{Err: err}
				return
			}
			if res != nil {
				mapperOutputs[j] = res.([]*rawQueryMapOutput)
			} else { // if we got a nil from the mapper it means that we've emptied all data from it
				mapperComplete[j] = true
			}
		}

		// process the mapper outputs. we can send out everything up to the min of the last time in the mappers
		min := int64(math.MaxInt64)
		for _, o := range mapperOutputs {
			// some of the mappers could empty out before others so ignore them because they'll be nil
			if o == nil {
				continue
			}

			// find the min of the last point in each mapper
			t := o[len(o)-1].Timestamp
			if t < min {
				min = t
			}
		}

		// now empty out all the mapper outputs up to the min time
		var values []*rawQueryMapOutput
		for j, o := range mapperOutputs {
			// find the index of the point up to the min
			ind := len(o)
			for i, mo := range o {
				if mo.Timestamp > min {
					ind = i
					break
				}
			}

			// add up to the index to the values
			values = append(values, o[:ind]...)

			// if we emptied out all the values, set this output to nil so that the mapper will get run again on the next loop
			if ind == len(o) {
				mapperOutputs[j] = nil
			}
		}

		// if we didn't pull out any values, we're done here
		if values == nil {
			break
		}

		// sort the values by time first so we can then handle offset and limit
		sort.Sort(rawOutputs(values))

		// get rid of any points that need to be offset
		if valuesOffset < m.stmt.Offset {
			offset := m.stmt.Offset - valuesOffset

			// if offset is bigger than the number of values we have, move to the next batch from the mappers
			if offset > len(values) {
				valuesOffset += len(values)
				continue
			}

			values = values[offset:]
			valuesOffset += offset
		}

		// ensure we don't send more than the limit
		if valuesSent < m.stmt.Limit {
			limit := m.stmt.Limit - valuesSent
			if len(values) > limit {
				values = values[:limit]
			}
			valuesSent += len(values)
		}

		valuesToReturn = append(valuesToReturn, values...)

		// hit the chunk size? Send out what has been accumulated, but keep
		// processing.
		if len(valuesToReturn) >= m.chunkSize {
			row := m.processRawResults(valuesToReturn)
			// perform post-processing, such as math.
			row.Values = m.processResults(row.Values)
			out <- row
			valuesToReturn = make([]*rawQueryMapOutput, 0)
		}

		// stop processing if we've hit the limit
		if m.stmt.Limit != 0 && valuesSent >= m.stmt.Limit {
			break
		}
	}

	if len(valuesToReturn) == 0 {
		if !filterEmptyResults {
			out <- m.processRawResults(nil)
		}
	} else {
		row := m.processRawResults(valuesToReturn)
		// perform post-processing, such as math.
		row.Values = m.processResults(row.Values)
		out <- row
	}
}

// processsResults will apply any math that was specified in the select statement against the passed in results
func (m *MapReduceJob) processResults(results [][]interface{}) [][]interface{} {
	hasMath := false
	for _, f := range m.stmt.Fields {
		if _, ok := f.Expr.(*BinaryExpr); ok {
			hasMath = true
		} else if _, ok := f.Expr.(*ParenExpr); ok {
			hasMath = true
		}
	}

	if !hasMath {
		return results
	}

	processors := make([]processor, len(m.stmt.Fields))
	startIndex := 1
	for i, f := range m.stmt.Fields {
		processors[i], startIndex = getProcessor(f.Expr, startIndex)
	}

	mathResults := make([][]interface{}, len(results))
	for i, _ := range mathResults {
		mathResults[i] = make([]interface{}, len(m.stmt.Fields)+1)
		// put the time in
		mathResults[i][0] = results[i][0]
		for j, p := range processors {
			mathResults[i][j+1] = p(results[i])
		}
	}

	return mathResults
}

// processFill will take the results and return new reaults (or the same if no fill modifications are needed) with whatever fill options the query has.
func (m *MapReduceJob) processFill(results [][]interface{}) [][]interface{} {
	// don't do anything if we're supposed to leave the nulls
	if m.stmt.Fill == NullFill {
		return results
	}

	if m.stmt.Fill == NoFill {
		// remove any rows that have even one nil value. This one is tricky because they could have multiple
		// aggregates, but this option means that any row that has even one nil gets purged.
		newResults := make([][]interface{}, 0, len(results))
		for _, vals := range results {
			hasNil := false
			// start at 1 because the first value is always time
			for j := 1; j < len(vals); j++ {
				if vals[j] == nil {
					hasNil = true
					break
				}
			}
			if !hasNil {
				newResults = append(newResults, vals)
			}
		}
		return newResults
	}

	// they're either filling with previous values or a specific number
	for i, vals := range results {
		// start at 1 because the first value is always time
		for j := 1; j < len(vals); j++ {
			if vals[j] == nil {
				switch m.stmt.Fill {
				case PreviousFill:
					if i != 0 {
						vals[j] = results[i-1][j]
					}
				case NumberFill:
					vals[j] = m.stmt.FillValue
				}
			}
		}
	}
	return results
}

func getProcessor(expr Expr, startIndex int) (processor, int) {
	switch expr := expr.(type) {
	case *VarRef:
		return newEchoProcessor(startIndex), startIndex + 1
	case *Call:
		return newEchoProcessor(startIndex), startIndex + 1
	case *BinaryExpr:
		return getBinaryProcessor(expr, startIndex)
	case *ParenExpr:
		return getProcessor(expr.Expr, startIndex)
	case *NumberLiteral:
		return newLiteralProcessor(expr.Val), startIndex
	case *StringLiteral:
		return newLiteralProcessor(expr.Val), startIndex
	case *BooleanLiteral:
		return newLiteralProcessor(expr.Val), startIndex
	case *TimeLiteral:
		return newLiteralProcessor(expr.Val), startIndex
	case *DurationLiteral:
		return newLiteralProcessor(expr.Val), startIndex
	}
	panic("unreachable")
}

type processor func(values []interface{}) interface{}

func newEchoProcessor(index int) processor {
	return func(values []interface{}) interface{} {
		return values[index]
	}
}

func newLiteralProcessor(val interface{}) processor {
	return func(values []interface{}) interface{} {
		return val
	}
}

func getBinaryProcessor(expr *BinaryExpr, startIndex int) (processor, int) {
	lhs, index := getProcessor(expr.LHS, startIndex)
	rhs, index := getProcessor(expr.RHS, index)

	return newBinaryExprEvaluator(expr.Op, lhs, rhs), index
}

func newBinaryExprEvaluator(op Token, lhs, rhs processor) processor {
	switch op {
	case ADD:
		return func(values []interface{}) interface{} {
			l := lhs(values)
			r := rhs(values)
			if lv, ok := l.(float64); ok {
				if rv, ok := r.(float64); ok {
					if rv != 0 {
						return lv + rv
					}
				}
			}
			return nil
		}
	case SUB:
		return func(values []interface{}) interface{} {
			l := lhs(values)
			r := rhs(values)
			if lv, ok := l.(float64); ok {
				if rv, ok := r.(float64); ok {
					if rv != 0 {
						return lv - rv
					}
				}
			}
			return nil
		}
	case MUL:
		return func(values []interface{}) interface{} {
			l := lhs(values)
			r := rhs(values)
			if lv, ok := l.(float64); ok {
				if rv, ok := r.(float64); ok {
					if rv != 0 {
						return lv * rv
					}
				}
			}
			return nil
		}
	case DIV:
		return func(values []interface{}) interface{} {
			l := lhs(values)
			r := rhs(values)
			if lv, ok := l.(float64); ok {
				if rv, ok := r.(float64); ok {
					if rv != 0 {
						return lv / rv
					}
				}
			}
			return nil
		}
	default:
		// we shouldn't get here, but give them back nils if it goes this way
		return func(values []interface{}) interface{} {
			return nil
		}
	}
}

// resultsEmpty will return true if the all the result values are empty or contain only nulls
func (m *MapReduceJob) resultsEmpty(resultValues [][]interface{}) bool {
	for _, vals := range resultValues {
		// start the loop at 1 because we want to skip over the time value
		for i := 1; i < len(vals); i++ {
			if vals[i] != nil {
				return false
			}
		}
	}
	return true
}

// processRawResults will handle converting the reduce results from a raw query into a Row
func (m *MapReduceJob) processRawResults(values []*rawQueryMapOutput) *Row {
	selectNames := m.stmt.NamesInSelect()

	// ensure that time is in the select names and in the first position
	hasTime := false
	for i, n := range selectNames {
		if n == "time" {
			if i != 0 {
				tmp := selectNames[0]
				selectNames[0] = "time"
				selectNames[i] = tmp
			}
			hasTime = true
		}
	}

	// time should always be in the list of names they get back
	if !hasTime {
		selectNames = append([]string{"time"}, selectNames...)
	}

	// if they've selected only a single value we have to handle things a little differently
	singleValue := len(selectNames) == SelectColumnCountWithOneValue

	row := &Row{
		Name:    m.MeasurementName,
		Tags:    m.TagSet.Tags,
		Columns: selectNames,
	}

	// return an empty row if there are no results
	if len(values) == 0 {
		return row
	}

	// the results will have all of the raw mapper results, convert into the row
	for _, v := range values {
		vals := make([]interface{}, len(selectNames))

		if singleValue {
			vals[0] = time.Unix(0, v.Timestamp).UTC()
			vals[1] = v.Values.(interface{})
		} else {
			fields := v.Values.(map[string]interface{})

			// time is always the first value
			vals[0] = time.Unix(0, v.Timestamp).UTC()

			// populate the other values
			for i := 1; i < len(selectNames); i++ {
				vals[i] = fields[selectNames[i]]
			}
		}

		row.Values = append(row.Values, vals)
	}

	return row
}

func (m *MapReduceJob) processAggregate(c *Call, reduceFunc ReduceFunc, resultValues [][]interface{}) error {
	mapperOutputs := make([]interface{}, len(m.Mappers))

	// intialize the mappers
	for _, mm := range m.Mappers {
		// for aggregate queries, we use the chunk size to determine how many times NextInterval should be called.
		// This is the number of buckets that we need to fill.
		if err := mm.Begin(c, m.TMin, len(resultValues)); err != nil {
			return err
		}
	}

	// populate the result values for each interval of time
	for i, _ := range resultValues {
		// collect the results from each mapper
		for j, mm := range m.Mappers {
			res, err := mm.NextInterval()
			if err != nil {
				return err
			}
			mapperOutputs[j] = res
		}
		resultValues[i] = append(resultValues[i], reduceFunc(mapperOutputs))
	}

	return nil
}

type MapReduceJobs []*MapReduceJob

func (a MapReduceJobs) Len() int           { return len(a) }
func (a MapReduceJobs) Less(i, j int) bool { return bytes.Compare(a[i].Key(), a[j].Key()) == -1 }
func (a MapReduceJobs) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

// Mapper will run through a map function. A single mapper will be created
// for each shard for each tagset that must be hit to satisfy a query.
// Mappers can either point to a local shard or could point to a remote server.
type Mapper interface {
	// Open will open the necessary resources to being the map job. Could be connections to remote servers or
	// hitting the local bolt store
	Open() error

	// Close will close the mapper (either the bolt transaction or the request)
	Close()

	// Begin will set up the mapper to run the map function for a given aggregate call starting at the passed in time.
	// For raw data queries it will yield to the mapper no more than limit number of points.
	Begin(aggregate *Call, startingTime int64, limit int) error

	// NextInterval will get the time ordered next interval of the given interval size from the mapper. This is a
	// forward only operation from the start time passed into Begin. Will return nil when there is no more data to be read.
	// Interval periods can be different based on time boundaries (months, daylight savings, etc) of the query.
	NextInterval() (interface{}, error)
}

type TagSet struct {
	Tags      map[string]string
	Filters   []Expr
	SeriesIDs []uint64
	Key       []byte
}

func (t *TagSet) AddFilter(id uint64, filter Expr) {
	t.SeriesIDs = append(t.SeriesIDs, id)
	t.Filters = append(t.Filters, filter)
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
func (p *Planner) Plan(stmt *SelectStatement, chunkSize int) (*Executor, error) {
	now := p.Now().UTC()

	// Replace instances of "now()" with the current time.
	stmt.Condition = Reduce(stmt.Condition, &NowValuer{Now: now})

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
	jobs, err := tx.CreateMapReduceJobs(stmt, tags)
	if err != nil {
		return nil, err
	}

	// LIMIT and OFFSET the unique series
	if stmt.SLimit > 0 || stmt.SOffset > 0 {
		if stmt.SOffset > len(jobs) {
			jobs = nil
		} else {
			if stmt.SOffset+stmt.SLimit > len(jobs) {
				stmt.SLimit = len(jobs) - stmt.SOffset
			}

			jobs = jobs[stmt.SOffset : stmt.SOffset+stmt.SLimit]
		}
	}

	for _, j := range jobs {
		j.interval = interval.Nanoseconds()
		j.stmt = stmt
		j.chunkSize = chunkSize
	}

	return &Executor{tx: tx, stmt: stmt, jobs: jobs, interval: interval.Nanoseconds()}, nil
}

// Executor represents the implementation of Executor.
// It executes all reducers and combines their result into a row.
type Executor struct {
	tx       Tx               // transaction
	stmt     *SelectStatement // original statement
	jobs     []*MapReduceJob  // one job per unique tag set that will return in the query
	interval int64            // the group by interval of the query in nanoseconds
}

// Execute begins execution of the query and returns a channel to receive rows.
func (e *Executor) Execute() <-chan *Row {
	// Create output channel and stream data in a separate goroutine.
	out := make(chan *Row, 0)
	go e.execute(out)

	return out
}

func (e *Executor) close() {
	for _, j := range e.jobs {
		j.Close()
	}
}

// execute runs in a separate separate goroutine and streams data from processors.
func (e *Executor) execute(out chan *Row) {
	// Ensure the the MRJobs close after execution.
	defer e.close()

	// If we have multiple tag sets we'll want to filter out the empty ones
	filterEmptyResults := len(e.jobs) > 1

	// Execute each MRJob serially
	for _, j := range e.jobs {
		j.Execute(out, filterEmptyResults)
	}

	// Mark the end of the output channel.
	close(out)
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
