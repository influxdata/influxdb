package tsdb

import (
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"sort"
	"time"

	"github.com/influxdb/influxdb/influxql"
	"github.com/influxdb/influxdb/meta"
)

type Mapper interface {
	// Open will open the necessary resources to being the map job. Could be connections to remote servers or
	// hitting the local store
	Open() error

	// Close will close the mapper
	Close()

	// TagSets returns the tagsets for which the Mapper has data.
	TagSets() []string

	// NextChunk returns the next chunk of points for the given tagset. The chunk will be a maximum size
	// of chunkSize, but it may be less. A chunkSize of 0 means do no chunking.
	NextChunk(tagset string, chunkSize int) (*rawMapperOutput, error)
}

type Executor interface {
	Execute(chunkSize int) <-chan *influxql.Row
}

type Planner struct {
	MetaStore interface {
		ShardGroupsByTimeRange(database, policy string, min, max time.Time) (a []meta.ShardGroupInfo, err error)
		NodeID() uint64
	}

	Cluster interface {
		NewRawMapper(shardID uint64, stmt string) (*RawMapper, error)
		NewAggMapper(shardID uint64, stmt string) (*AggMapper, error)
	}

	store *Store

	Logger *log.Logger
}

func NewPlanner(store *Store) *Planner {
	return &Planner{
		store:  store,
		Logger: log.New(os.Stderr, "[planner] ", log.LstdFlags),
	}
}

// Plan creates an execution plan for the given SelectStatement and returns an Executor.
func (p *Planner) Plan(stmt *influxql.SelectStatement) (Executor, error) {
	shards := map[uint64]meta.ShardInfo{} // Shards requiring mappers.

	for _, src := range stmt.Sources {
		mm, ok := src.(*influxql.Measurement)
		if !ok {
			return nil, fmt.Errorf("invalid source type: %#v", src)
		}

		// Replace instances of "now()" with the current time, and check the resultant times.
		stmt.Condition = influxql.Reduce(stmt.Condition, &influxql.NowValuer{Now: time.Now().UTC()})
		tmin, tmax := influxql.TimeRange(stmt.Condition)
		if tmax.IsZero() {
			tmax = time.Now()
		}
		if tmin.IsZero() {
			tmin = time.Unix(0, 0)
		}

		// Build the set of target shards. Using shard IDs as keys ensures each shard ID
		// occurs only once.
		shardGroups, err := p.MetaStore.ShardGroupsByTimeRange(mm.Database, mm.RetentionPolicy, tmin, tmax)
		if err != nil {
			return nil, err
		}
		for _, g := range shardGroups {
			for _, sh := range g.Shards {
				shards[sh.ID] = sh
			}
		}
	}

	if stmt.IsRawQuery && !stmt.HasDistinct() {
		return p.planRawQuery(stmt, shards)
	}
	return p.planAggregateQuery(stmt, shards)
}

func (p *Planner) planRawQuery(stmt *influxql.SelectStatement, shards map[uint64]meta.ShardInfo) (Executor, error) {
	// Build the Mappers, one per shard. If the shard is local to this node, always use
	// that one, versus asking the cluster.
	mappers := []*RawMapper{}
	for _, sh := range shards {
		if sh.OwnedBy(p.MetaStore.NodeID()) {
			shard := p.store.Shard(sh.ID)
			if shard == nil {
				// If the store returns nil, no data has actually been written to the shard.
				// In this case, since there is no data, don't make a mapper.
				continue
			}
			mappers = append(mappers, NewRawMapper(shard, stmt))
		} else {
			mapper, err := p.Cluster.NewRawMapper(sh.ID, stmt.String())
			if err != nil {
				return nil, err
			}
			mappers = append(mappers, mapper)
		}
	}

	return NewRawExecutor(stmt, mappers), nil
}

func (p *Planner) planAggregateQuery(stmt *influxql.SelectStatement, shards map[uint64]meta.ShardInfo) (Executor, error) {
	// Build the Mappers, one per shard. If the shard is local to this node, always use
	// that one, versus asking the cluster.
	mappers := []*AggMapper{}
	for _, sh := range shards {
		if sh.OwnedBy(p.MetaStore.NodeID()) {
			shard := p.store.Shard(sh.ID)
			if shard == nil {
				// If the store returns nil, no data has actually been written to the shard.
				// In this case, since there is no data, don't make a mapper.
				continue
			}
			mappers = append(mappers, NewAggMapper(shard, stmt))
		} else {
			mapper, err := p.Cluster.NewAggMapper(sh.ID, stmt.String())
			if err != nil {
				return nil, err
			}
			mappers = append(mappers, mapper)
		}
	}

	return NewAggregateExecutor(stmt, mappers), nil
}

// RawExecutor is an executor for RawMappers.
type RawExecutor struct {
	stmt    *influxql.SelectStatement
	mappers []*RawMapper
}

// NewRawExecutor returns a new RawExecutor.
func NewRawExecutor(stmt *influxql.SelectStatement, mappers []*RawMapper) *RawExecutor {
	return &RawExecutor{
		stmt:    stmt,
		mappers: mappers,
	}
}

// Execute begins execution of the query and returns a channel to receive rows.
func (re *RawExecutor) Execute(chunkSize int) <-chan *influxql.Row {
	// Create output channel and stream data in a separate goroutine.
	out := make(chan *influxql.Row, 0)
	go re.execute(out, chunkSize)
	return out
}

func (re *RawExecutor) execute(out chan *influxql.Row, chunkSize int) {
	// It's important that all resources are released when execution completes.
	defer re.close()

	// Open the mappers.
	for _, m := range re.mappers {
		if err := m.Open(); err != nil {
			out <- &influxql.Row{Err: err}
			return
		}
	}

	// Build the set of available tagsets across all mappers.
	availTagSets := newStringSet()
	for _, m := range re.mappers {
		for _, t := range m.TagSets() {
			availTagSets.add(t)
		}
	}

tagsetLoop:
	// For each mapper, drain it by tagset.
	for _, t := range availTagSets.list() {
		// Used to read ahead chunks.
		mapperOutputs := make([]*rawMapperOutput, len(re.mappers))
		var rowWriter *limitedRowWriter

		for {
			// Get the next chunk from each mapper.
			for j, m := range re.mappers {
				if mapperOutputs[j] != nil {
					// There is data for this mapper, for this tagset, from a previous loop.
					continue
				}

				chunk, err := m.NextChunk(t, chunkSize)
				if err != nil {
					out <- &influxql.Row{Err: err}
					return
				}

				// Now that we have full name and tag details, initialize the rowWriter.
				// The Name and Tags will be the same for all mappers.
				if rowWriter == nil {
					rowWriter = &limitedRowWriter{
						limit:       re.stmt.Limit,
						chunkSize:   chunkSize,
						name:        chunk.Name,
						tags:        chunk.Tags,
						selectNames: re.stmt.NamesInSelect(),
						fields:      re.stmt.Fields,
						c:           out,
					}
				}

				if len(chunk.Values) == 0 {
					// This mapper is empty for this tagset.
					continue
				}
				mapperOutputs[j] = chunk
			}

			// Process the mapper outputs. We can send out everything up to the min of the last time
			// of the chunks.
			minTime := int64(math.MaxInt64)
			for _, o := range mapperOutputs {
				// Some of the mappers could empty out before others so ignore them because they'll be nil
				if o == nil {
					continue
				}

				// Find the min of the last point across all the chunks.
				t := o.Values[len(o.Values)-1].Time
				if t < minTime {
					minTime = t
				}
			}

			// Now empty out all the chunks up to the min time. Create new output struct for this data.
			var chunkedOutput *rawMapperOutput
			for j, o := range mapperOutputs {
				if o == nil {
					continue
				}

				// Very first value in this mapper is at a higher timestamp. Skip it.
				if o.Values[0].Time > minTime {
					continue
				}

				// Find the index of the point up to the min.
				ind := len(o.Values)
				for i, mo := range o.Values {
					if mo.Time > minTime {
						ind = i
						break
					}
				}

				// Add up to the index to the values
				if chunkedOutput == nil {
					chunkedOutput = &rawMapperOutput{
						Name: o.Name,
						Tags: o.Tags,
					}
					chunkedOutput.Values = o.Values[:ind]
				} else {
					chunkedOutput.Values = append(chunkedOutput.Values, o.Values[:ind]...)
				}

				// Clear out the values being sent out, keep the remainder.
				mapperOutputs[j].Values = mapperOutputs[j].Values[ind:]

				// If we emptied out all the values, set this output to nil so that the mapper will get run again on the next loop
				if len(mapperOutputs[j].Values) == 0 {
					mapperOutputs[j] = nil
				}
			}

			// If we didn't pull out any values, this tagset is done.
			if chunkedOutput == nil {
				break
			}

			// Sort the values by time first so we can then handle offset and limit
			sort.Sort(rawMapperValues(chunkedOutput.Values))

			if limited := rowWriter.Add(chunkedOutput.Values); limited {
				// Limit for this tagset was reached, go to next one.
				continue tagsetLoop
			}
		}

		// Be sure to kick out any residual values.
		rowWriter.Flush()
	}

	// XXX Limit and chunk checking.
	close(out)
}

// Close closes the executor such that all resources are released. Once closed,
// an executor may not be re-used.
func (re *RawExecutor) close() {
	if re != nil {
		for _, m := range re.mappers {
			m.Close()
		}
	}
}

// AggregateExecutor is an executor for AggregateMappers.
type AggregateExecutor struct {
	stmt    *influxql.SelectStatement
	mappers []*AggMapper
}

// NewAggregateExecutor returns a new AggregateExecutor.
func NewAggregateExecutor(stmt *influxql.SelectStatement, mappers []*AggMapper) *AggregateExecutor {
	return &AggregateExecutor{
		stmt:    stmt,
		mappers: mappers,
	}
}

// Execute begins execution of the query and returns a channel to receive rows.
func (ae *AggregateExecutor) Execute(chunkSize int) <-chan *influxql.Row {
	// Create output channel and stream data in a separate goroutine.
	out := make(chan *influxql.Row, 0)
	go ae.execute(out, chunkSize)
	return out
}

func (ae *AggregateExecutor) execute(out chan *influxql.Row, chunkSize int) {
	// Open the mappers.
	for _, m := range ae.mappers {
		if err := m.Open(); err != nil {
			out <- &influxql.Row{Err: err}
			return
		}
	}

	// Build the set of available tagsets across all mappers.
	availTagSets := newStringSet()
	for _, m := range ae.mappers {
		for _, t := range m.TagSets() {
			availTagSets.add(t)
		}
	}

	// Get the aggregates and the associated reduce functions
	aggregates := ae.stmt.FunctionCalls()
	reduceFuncs := make([]influxql.ReduceFunc, len(aggregates))
	for i, c := range aggregates {
		reduceFunc, err := influxql.InitializeReduceFunc(c)
		if err != nil {
			out <- &influxql.Row{Err: err}
			return
		}
		reduceFuncs[i] = reduceFunc
	}

	// Work out how many intervals we need per tagset. If the user didn't specify a start time or
	// GROUP BY interval, we're returning a single point per tagset, for the entire range.
	var tMins []int64
	var tmin, tmax int64
	qMin, qMax := influxql.TimeRange(ae.stmt.Condition)
	if qMin.IsZero() {
		tmin = time.Unix(0, 0).UnixNano()
	} else {
		tmin = qMin.UnixNano()
	}
	if qMax.IsZero() {
		tmax = time.Now().UnixNano()
	} else {
		tmax = qMax.UnixNano()
	}

	d, err := ae.stmt.GroupByInterval()
	if err != nil {
		out <- &influxql.Row{Err: err}
		return
	}
	interval := d.Nanoseconds()
	if tmin == 0 || interval == 0 {
		tMins = make([]int64, 1)
		interval = tmax - tmin
	} else {
		intervalTop := tmax/interval*interval + interval
		intervalBottom := tmin / interval * interval
		tMins = make([]int64, int((intervalTop-intervalBottom)/interval))
	}

	// For GROUP BY time queries, limit the number of data points returned by the limit and offset
	if ae.stmt.Limit > 0 || ae.stmt.Offset > 0 {
		// ensure that the offset isn't higher than the number of points we'd get
		if ae.stmt.Offset > len(tMins) {
			return
		}

		// Take the lesser of either the pre computed number of GROUP BY buckets that
		// will be in the result or the limit passed in by the user
		if ae.stmt.Limit < len(tMins) {
			tMins = tMins[:ae.stmt.Limit]
		}
	}

	// If we are exceeding our MaxGroupByPoints error out
	if len(tMins) > influxql.MaxGroupByPoints {
		out <- &influxql.Row{
			Err: errors.New("too many points in the group by interval. maybe you forgot to specify a where time clause?"),
		}
		return
	}

	// Ensure that the start time for the results is on the start of the window.
	startTime := tmin
	if interval > 0 {
		startTime = startTime / interval * interval
	}

	// Create the minimum times for each interval.
	for i, _ := range tMins {
		var t int64
		if ae.stmt.Offset > 0 {
			t = startTime + (int64(i+1) * interval * int64(ae.stmt.Offset))
		} else {
			t = startTime + (int64(i+1) * interval) - interval
		}
		tMins[i] = t

		// If we start getting out of our max time range, then truncate values and return
		//if t > tmax {
		//	resultValues = resultValues[:i]
		//	break
		//}

		// we always include time so we need one more column than we have aggregates
		//vals := make([]interface{}, 0, len(aggregates)+1)
		//resultValues[i] = append(vals, time.Unix(0, t).UTC())
	}

	// This just makes sure that if they specify a start time less than what the start time would be with the offset,
	// we just reset the start time to the later time to avoid going over data that won't show up in the result.
	//if m.stmt.Offset > 0 {
	//	m.TMin = resultValues[0][0].(time.Time).UnixNano()
	//}

	// Put together the rows to return, starting with columns.
	columnNames := make([]string, len(ae.stmt.Fields)+1)
	columnNames[0] = "time"
	for i, f := range ae.stmt.Fields {
		columnNames[i+1] = f.Name()
	}

	for _, tag := range availTagSets.list() {
		var err error
		var reducedVal interface{}
		measurement := ""
		tags := make(map[string]string)
		values := make([][]interface{}, len(tMins))

		for i, t := range tMins {
			values[i] = make([]interface{}, 0, len(columnNames))
			values[i] = append(values[i], time.Unix(0, t).UTC()) // Time value is always first.
			for j, c := range aggregates {
				measurement, tags, reducedVal, err = ae.processAggregate(tag, c, reduceFuncs[j], t, t+interval)
				if err != nil {
					out <- &influxql.Row{Err: err}
					return
				}
				values[i] = append(values[i], reducedVal)
			}
		}

		// handle any fill options
		values = ae.processFill(values)

		row := &influxql.Row{
			Name:    measurement,
			Tags:    tags,
			Columns: columnNames,
			Values:  values,
		}
		out <- row
	}

	close(out)
}

func (ae *AggregateExecutor) processAggregate(t string, c *influxql.Call, f influxql.ReduceFunc, tmin, tmax int64) (string, map[string]string, interface{}, error) {
	measurement := ""
	tags := make(map[string]string)
	mappedValues := make([]interface{}, 0, len(ae.mappers))
	for _, m := range ae.mappers {
		output, err := m.Interval(t, c, tmin, tmax)
		if err != nil {
			return "", nil, nil, err
		}
		if measurement == "" {
			measurement, tags = output.Name, output.Tags
		}
		mappedValues = append(mappedValues, output.Value)
	}
	return measurement, tags, f(mappedValues), nil
}

// processFill will take the results and return new results (or the same if no fill modifications are needed) with whatever fill options the query has.
func (ae *AggregateExecutor) processFill(results [][]interface{}) [][]interface{} {
	// don't do anything if we're supposed to leave the nulls
	if ae.stmt.Fill == influxql.NullFill {
		return results
	}

	if ae.stmt.Fill == influxql.NoFill {
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
				switch ae.stmt.Fill {
				case influxql.PreviousFill:
					if i != 0 {
						vals[j] = results[i-1][j]
					}
				case influxql.NumberFill:
					vals[j] = ae.stmt.FillValue
				}
			}
		}
	}
	return results
}

// Close closes the executor such that all resources are released. Once closed,
// an executor may not be re-used.
func (ae *AggregateExecutor) close() {
	for _, m := range ae.mappers {
		m.Close()
	}
}

// limitedRowWriter accepts raw maper values, and will emit those values as rows in chunks
// of the given size. If the chunk size is 0, no chunking will be performed. In addiiton if
// limit is reached, outstanding values will be emitted. If limit is zero, no limit is enforced.
type limitedRowWriter struct {
	chunkSize   int
	limit       int
	name        string
	tags        map[string]string
	selectNames []string
	fields      influxql.Fields
	c           chan *influxql.Row

	currValues []*rawMapperValue
	totalSent  int
}

// Add accepts a slice of values, and will emit those values as per chunking requirements.
// If limited is returned as true, the limit was also reached and no more values should be
// add. In that case only up the limit of values are emitted.
func (r *limitedRowWriter) Add(values []*rawMapperValue) (limited bool) {
	if r.currValues == nil {
		r.currValues = make([]*rawMapperValue, 0, r.chunkSize)
	}
	r.currValues = append(r.currValues, values...)

	// Check limit.
	limitReached := r.limit > 0 && r.totalSent+len(r.currValues) >= r.limit
	if limitReached {
		// Limit will be satified with current values. Truncate 'em.
		r.currValues = r.currValues[:r.limit-r.totalSent]
	}

	// Is chunking in effect?
	if r.chunkSize > 0 {
		// Chunking level reached?
		for len(r.currValues) >= r.chunkSize {
			index := len(r.currValues) - (len(r.currValues) - r.chunkSize)
			r.c <- r.processValues(r.currValues[:index])
			r.totalSent += index
			r.currValues = r.currValues[index:]
		}

		// After values have been sent out by chunking, there may still be some
		// values left, if the remainder is less than the chunk size. But if the
		// limit has been reached, kick them out.
		if len(r.currValues) > 0 && limitReached {
			r.c <- r.processValues(r.currValues)
			r.currValues = nil
		}
	} else if limitReached {
		// No chunking in effect, but the limit has been reached.
		r.c <- r.processValues(r.currValues)
		r.currValues = nil
	}

	return limitReached
}

// Flush instructs the limitedRowWriter to emit any pending values as a single row,
// adhering to any limits. Chunking is not enforced.
func (r *limitedRowWriter) Flush() {
	// If at least some rows were sent, and no values are pending, then don't
	// emit anything, since at least 1 row was previously emitted. This ensures
	// that if no rows were ever sent, at least 1 will be emitted, even an empty row.
	if r.totalSent != 0 && len(r.currValues) == 0 {
		return
	}

	if r.limit > 0 && len(r.currValues) > r.limit {
		r.currValues = r.currValues[:r.limit]
	}
	r.c <- r.processValues(r.currValues)
	r.totalSent = len(r.currValues)
	r.currValues = nil
}

// processValues emits the given values in a single row.
func (r *limitedRowWriter) processValues(values []*rawMapperValue) *influxql.Row {
	selectNames := r.selectNames

	// ensure that time is in the select names and in the first position
	hasTime := false
	for i, n := range selectNames {
		if n == "time" {
			// Swap time to the first argument for names
			if i != 0 {
				selectNames[0], selectNames[i] = selectNames[i], selectNames[0]
			}
			hasTime = true
			break
		}
	}

	// time should always be in the list of names they get back
	if !hasTime {
		selectNames = append([]string{"time"}, selectNames...)
	}

	// since selectNames can contain tags, we need to strip them out
	selectFields := make([]string, 0, len(selectNames))

	for _, n := range selectNames {
		if _, found := r.tags[n]; !found {
			selectFields = append(selectFields, n)
		}
	}

	row := &influxql.Row{
		Name:    r.name,
		Tags:    r.tags,
		Columns: selectFields,
	}

	// Kick out an empty row it no results available.
	if len(values) == 0 {
		return row
	}

	// if they've selected only a single value we have to handle things a little differently
	singleValue := len(selectFields) == influxql.SelectColumnCountWithOneValue

	// the results will have all of the raw mapper results, convert into the row
	for _, v := range values {
		vals := make([]interface{}, len(selectFields))

		if singleValue {
			vals[0] = time.Unix(0, v.Time).UTC()
			vals[1] = v.Value.(interface{})
		} else {
			fields := v.Value.(map[string]interface{})

			// time is always the first value
			vals[0] = time.Unix(0, v.Time).UTC()

			// populate the other values
			for i := 1; i < len(selectFields); i++ {
				vals[i] = fields[selectFields[i]]
			}
		}

		row.Values = append(row.Values, vals)
	}

	// Perform any mathematical post-processing.
	row.Values = r.processRowValues(row.Values)

	return row
}

// processRowValues will apply any math that was specified in the select statement against the passed in results
func (r *limitedRowWriter) processRowValues(results [][]interface{}) [][]interface{} {
	hasMath := false
	for _, f := range r.fields {
		if _, ok := f.Expr.(*influxql.BinaryExpr); ok {
			hasMath = true
		} else if _, ok := f.Expr.(*influxql.ParenExpr); ok {
			hasMath = true
		}
	}

	if !hasMath {
		return results
	}

	processors := make([]influxql.Processor, len(r.fields))
	startIndex := 1
	for i, f := range r.fields {
		processors[i], startIndex = influxql.GetProcessor(f.Expr, startIndex)
	}

	mathResults := make([][]interface{}, len(results))
	for i, _ := range mathResults {
		mathResults[i] = make([]interface{}, len(r.fields)+1)
		// put the time in
		mathResults[i][0] = results[i][0]
		for j, p := range processors {
			mathResults[i][j+1] = p(results[i])
		}
	}

	return mathResults
}
