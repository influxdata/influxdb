package tsdb

import (
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/influxdb/influxdb/influxql"
	"github.com/influxdb/influxdb/models"
)

const (
	// Return an error if the user is trying to select more than this number of points in a group by statement.
	// Most likely they specified a group by interval without time boundaries.
	MaxGroupByPoints = 100000

	// Since time is always selected, the column count when selecting only a single other value will be 2
	SelectColumnCountWithOneValue = 2

	// IgnoredChunkSize is what gets passed into Mapper.Begin for aggregate queries as they don't chunk points out
	IgnoredChunkSize = 0
)

// Executor is an interface for a query executor.
type Executor interface {
	Execute() <-chan *models.Row
}

type SelectExecutor struct {
	stmt           *influxql.SelectStatement
	mappers        []*StatefulMapper
	chunkSize      int
	limitedTagSets map[string]struct{} // Set tagsets for which data has reached the LIMIT.
}

// NewSelectExecutor returns a new SelectExecutor.
func NewSelectExecutor(stmt *influxql.SelectStatement, mappers []Mapper, chunkSize int) *SelectExecutor {
	a := []*StatefulMapper{}
	for _, m := range mappers {
		a = append(a, &StatefulMapper{m, nil, false})
	}
	return &SelectExecutor{
		stmt:           stmt,
		mappers:        a,
		chunkSize:      chunkSize,
		limitedTagSets: make(map[string]struct{}),
	}
}

// Execute begins execution of the query and returns a channel to receive rows.
func (e *SelectExecutor) Execute() <-chan *models.Row {
	// Create output channel and stream data in a separate goroutine.
	out := make(chan *models.Row, 0)

	// Certain operations on the SELECT statement can be performed by the SelectExecutor without
	// assistance from the Mappers. This allows the SelectExecutor to prepare aggregation functions
	// and mathematical functions.
	e.stmt.RewriteDistinct()

	if (e.stmt.IsRawQuery && !e.stmt.HasDistinct()) || e.stmt.IsSimpleDerivative() {
		go e.executeRaw(out)
	} else {
		go e.executeAggregate(out)
	}
	return out
}

// mappersDrained returns whether all the executors Mappers have been drained of data.
func (e *SelectExecutor) mappersDrained() bool {
	for _, m := range e.mappers {
		if !m.drained {
			return false
		}
	}
	return true
}

// nextMapperTagset returns the alphabetically lowest tagset across all Mappers.
func (e *SelectExecutor) nextMapperTagSet() string {
	tagset := ""
	for _, m := range e.mappers {
		if m.bufferedChunk != nil {
			if tagset == "" {
				tagset = m.bufferedChunk.key()
			} else if m.bufferedChunk.key() < tagset {
				tagset = m.bufferedChunk.key()
			}
		}
	}
	return tagset
}

// nextMapperLowestTime returns the lowest minimum time across all Mappers, for the given tagset.
func (e *SelectExecutor) nextMapperLowestTime(tagset string) int64 {
	minTime := int64(math.MaxInt64)
	for _, m := range e.mappers {
		if !m.drained && m.bufferedChunk != nil {
			if m.bufferedChunk.key() != tagset {
				continue
			}
			t := m.bufferedChunk.Values[len(m.bufferedChunk.Values)-1].Time
			if t < minTime {
				minTime = t
			}
		}
	}
	return minTime
}

// nextMapperHighestTime returns the highest time across all Mappers, for the given tagset.
func (e *SelectExecutor) nextMapperHighestTime(tagset string) int64 {
	maxTime := int64(math.MinInt64)
	for _, m := range e.mappers {
		if !m.drained && m.bufferedChunk != nil {
			if m.bufferedChunk.key() != tagset {
				continue
			}
			t := m.bufferedChunk.Values[0].Time
			if t > maxTime {
				maxTime = t
			}
		}
	}
	return maxTime
}

// tagSetIsLimited returns whether data for the given tagset has been LIMITed.
func (e *SelectExecutor) tagSetIsLimited(tagset string) bool {
	_, ok := e.limitedTagSets[tagset]
	return ok
}

// limitTagSet marks the given taset as LIMITed.
func (e *SelectExecutor) limitTagSet(tagset string) {
	e.limitedTagSets[tagset] = struct{}{}
}

func (e *SelectExecutor) executeRaw(out chan *models.Row) {
	// It's important that all resources are released when execution completes.
	defer e.close()

	// Open the mappers.
	for _, m := range e.mappers {
		if err := m.Open(); err != nil {
			out <- &models.Row{Err: err}
			return
		}
	}

	// Get the distinct fields across all mappers.
	var selectFields, aliasFields []string
	if e.stmt.HasWildcard() {
		sf := newStringSet()
		for _, m := range e.mappers {
			sf.add(m.Fields()...)
		}
		selectFields = sf.list()
		aliasFields = selectFields
	} else {
		selectFields = e.stmt.Fields.Names()
		aliasFields = e.stmt.Fields.AliasNames()
	}

	// Used to read ahead chunks from mappers.
	var rowWriter *limitedRowWriter
	var currTagset string

	// Keep looping until all mappers drained.
	var err error
	for {
		// Get the next chunk from each Mapper.
		for _, m := range e.mappers {
			if m.drained {
				continue
			}

			// Set the next buffered chunk on the mapper, or mark it drained.
			for {
				if m.bufferedChunk == nil {
					m.bufferedChunk, err = m.NextChunk()
					if err != nil {
						out <- &models.Row{Err: err}
						return
					}
					if m.bufferedChunk == nil {
						// Mapper can do no more for us.
						m.drained = true
						break
					}

					// If the SELECT query is on more than 1 field, but the chunks values from the Mappers
					// only contain a single value, create k-v pairs using the field name of the chunk
					// and the value of the chunk. If there is only 1 SELECT field across all mappers then
					// there is no need to create k-v pairs, and there is no need to distinguish field data,
					// as it is all for the *same* field.
					if len(selectFields) > 1 && len(m.bufferedChunk.Fields) == 1 {
						fieldKey := m.bufferedChunk.Fields[0]

						for i := range m.bufferedChunk.Values {
							field := map[string]interface{}{fieldKey: m.bufferedChunk.Values[i].Value}
							m.bufferedChunk.Values[i].Value = field
						}
					}
				}

				if e.tagSetIsLimited(m.bufferedChunk.Name) {
					// chunk's tagset is limited, so no good. Try again.
					m.bufferedChunk = nil
					continue
				}
				// This mapper has a chunk available, and it is not limited.
				break
			}
		}

		// All Mappers done?
		if e.mappersDrained() {
			rowWriter.Flush()
			break
		}

		// Send out data for the next alphabetically-lowest tagset. All Mappers emit data in this order,
		// so by always continuing with the lowest tagset until it is finished, we process all data in
		// the required order, and don't "miss" any.
		tagset := e.nextMapperTagSet()
		if tagset != currTagset {
			currTagset = tagset
			// Tagset has changed, time for a new rowWriter. Be sure to kick out any residual values.
			rowWriter.Flush()
			rowWriter = nil
		}

		ascending := true
		if len(e.stmt.SortFields) > 0 {
			ascending = e.stmt.SortFields[0].Ascending
		}

		var timeBoundary int64

		if ascending {
			// Process the mapper outputs. We can send out everything up to the min of the last time
			// of the chunks for the next tagset.
			timeBoundary = e.nextMapperLowestTime(tagset)
		} else {
			timeBoundary = e.nextMapperHighestTime(tagset)
		}

		// Now empty out all the chunks up to the min time. Create new output struct for this data.
		var chunkedOutput *MapperOutput
		for _, m := range e.mappers {
			if m.drained {
				continue
			}

			chunkBoundary := false
			if ascending {
				chunkBoundary = m.bufferedChunk.Values[0].Time > timeBoundary
			} else {
				chunkBoundary = m.bufferedChunk.Values[0].Time < timeBoundary
			}

			// This mapper's next chunk is not for the next tagset, or the very first value of
			// the chunk is at a higher acceptable timestamp. Skip it.
			if m.bufferedChunk.key() != tagset || chunkBoundary {
				continue
			}

			// Find the index of the point up to the min.
			ind := len(m.bufferedChunk.Values)
			for i, mo := range m.bufferedChunk.Values {
				if ascending && mo.Time > timeBoundary {
					ind = i
					break
				} else if !ascending && mo.Time < timeBoundary {
					ind = i
					break
				}

			}

			// Add up to the index to the values
			if chunkedOutput == nil {
				chunkedOutput = &MapperOutput{
					Name:      m.bufferedChunk.Name,
					Tags:      m.bufferedChunk.Tags,
					cursorKey: m.bufferedChunk.key(),
				}
				chunkedOutput.Values = m.bufferedChunk.Values[:ind]
			} else {
				chunkedOutput.Values = append(chunkedOutput.Values, m.bufferedChunk.Values[:ind]...)
			}

			// Clear out the values being sent out, keep the remainder.
			m.bufferedChunk.Values = m.bufferedChunk.Values[ind:]

			// If we emptied out all the values, clear the mapper's buffered chunk.
			if len(m.bufferedChunk.Values) == 0 {
				m.bufferedChunk = nil
			}
		}

		if ascending {
			// Sort the values by time first so we can then handle offset and limit
			sort.Sort(MapperValues(chunkedOutput.Values))
		} else {
			sort.Sort(sort.Reverse(MapperValues(chunkedOutput.Values)))
		}

		// Now that we have full name and tag details, initialize the rowWriter.
		// The Name and Tags will be the same for all mappers.
		if rowWriter == nil {
			rowWriter = &limitedRowWriter{
				limit:       e.stmt.Limit,
				offset:      e.stmt.Offset,
				chunkSize:   e.chunkSize,
				name:        chunkedOutput.Name,
				tags:        chunkedOutput.Tags,
				selectNames: selectFields,
				aliasNames:  aliasFields,
				fields:      e.stmt.Fields,
				c:           out,
			}
		}
		if e.stmt.HasDerivative() {
			interval, err := derivativeInterval(e.stmt)
			if err != nil {
				out <- &models.Row{Err: err}
				return
			}
			rowWriter.transformer = &RawQueryDerivativeProcessor{
				IsNonNegative:      e.stmt.FunctionCalls()[0].Name == "non_negative_derivative",
				DerivativeInterval: interval,
			}
		}

		// Emit the data via the limiter.
		if limited := rowWriter.Add(chunkedOutput.Values); limited {
			// Limit for this tagset was reached, mark it and start draining a new tagset.
			e.limitTagSet(chunkedOutput.key())
			continue
		}
	}

	close(out)
}

func (e *SelectExecutor) executeAggregate(out chan *models.Row) {
	// It's important to close all resources when execution completes.
	defer e.close()

	// Create the functions which will reduce values from mappers for
	// a given interval. The function offsets within this slice match
	// the offsets within the value slices that are returned by the
	// mapper.
	aggregates := e.stmt.FunctionCalls()
	reduceFuncs := make([]reduceFunc, len(aggregates))
	for i, c := range aggregates {
		reduceFunc, err := initializeReduceFunc(c)
		if err != nil {
			out <- &models.Row{Err: err}
			return
		}
		reduceFuncs[i] = reduceFunc
	}

	// Put together the rows to return, starting with columns.
	columnNames := e.stmt.ColumnNames()

	// Open the mappers.
	for _, m := range e.mappers {
		if err := m.Open(); err != nil {
			out <- &models.Row{Err: err}
			return
		}
	}

	// Build the set of available tagsets across all mappers. This is used for
	// later checks.
	availTagSets := newStringSet()
	for _, m := range e.mappers {
		for _, t := range m.TagSets() {
			availTagSets.add(t)
		}
	}

	// Prime each mapper's chunk buffer.
	var err error
	for _, m := range e.mappers {
		m.bufferedChunk, err = m.NextChunk()
		if err != nil {
			out <- &models.Row{Err: err}
			return
		}
		if m.bufferedChunk == nil {
			m.drained = true
		}
	}

	ascending := true
	if len(e.stmt.SortFields) > 0 {
		ascending = e.stmt.SortFields[0].Ascending
	}

	// Keep looping until all mappers drained.
	for !e.mappersDrained() {
		// Send out data for the next alphabetically-lowest tagset. All Mappers send out in this order
		// so collect data for this tagset, ignoring all others.
		tagset := e.nextMapperTagSet()
		chunks := []*MapperOutput{}

		// Pull as much as possible from each mapper. Stop when a mapper offers
		// data for a new tagset, or empties completely.
		for _, m := range e.mappers {
			if m.drained {
				continue
			}

			for {
				if m.bufferedChunk == nil {
					m.bufferedChunk, err = m.NextChunk()
					if err != nil {
						out <- &models.Row{Err: err}
						return
					}
					if m.bufferedChunk == nil {
						m.drained = true
						break
					}
				}

				// Got a chunk. Can we use it?
				if m.bufferedChunk.key() != tagset {
					// No, so just leave it in the buffer.
					break
				}
				// We can, take it.
				chunks = append(chunks, m.bufferedChunk)
				m.bufferedChunk = nil
			}
		}

		// Prep a row, ready for kicking out.
		var row *models.Row

		// Prep for bucketing data by start time of the interval.
		buckets := map[int64][][]interface{}{}

		for _, chunk := range chunks {
			if row == nil {
				row = &models.Row{
					Name:    chunk.Name,
					Tags:    chunk.Tags,
					Columns: columnNames,
				}
			}

			startTime := chunk.Values[0].Time
			_, ok := buckets[startTime]
			values := chunk.Values[0].Value.([]interface{})
			if !ok {
				buckets[startTime] = make([][]interface{}, len(values))
			}
			for i, v := range values {
				buckets[startTime][i] = append(buckets[startTime][i], v)
			}
		}

		// Now, after the loop above, within each time bucket is a slice. Within the element of each
		// slice is another slice of interface{}, ready for passing to the reducer functions.

		// Work each bucket of time, in time ascending order.
		tMins := make(int64arr, 0, len(buckets))
		for k, _ := range buckets {
			tMins = append(tMins, k)
		}

		if ascending {
			sort.Sort(tMins)
		} else {
			sort.Sort(sort.Reverse(tMins))
		}

		values := make([][]interface{}, len(tMins))
		for i, t := range tMins {
			values[i] = make([]interface{}, 0, len(columnNames))
			values[i] = append(values[i], time.Unix(0, t).UTC()) // Time value is always first.

			for j, f := range reduceFuncs {
				reducedVal := f(buckets[t][j])
				values[i] = append(values[i], reducedVal)
			}
		}

		// Perform aggregate unwraps
		values, err = e.processFunctions(values, columnNames)
		if err != nil {
			out <- &models.Row{Err: err}
		}

		// Perform any mathematics.
		values = processForMath(e.stmt.Fields, values)

		// Handle any fill options
		values = e.processFill(values)

		// process derivatives
		values = e.processDerivative(values)

		// If we have multiple tag sets we'll want to filter out the empty ones
		if len(availTagSets) > 1 && resultsEmpty(values) {
			continue
		}

		row.Values = values
		out <- row
	}

	close(out)
}

// processFill will take the results and return new results (or the same if no fill modifications are needed)
// with whatever fill options the query has.
func (e *SelectExecutor) processFill(results [][]interface{}) [][]interface{} {
	// don't do anything if we're supposed to leave the nulls
	if e.stmt.Fill == influxql.NullFill {
		return results
	}

	if e.stmt.Fill == influxql.NoFill {
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

	// They're either filling with previous values or a specific number
	for i, vals := range results {
		// start at 1 because the first value is always time
		for j := 1; j < len(vals); j++ {
			if vals[j] == nil {
				switch e.stmt.Fill {
				case influxql.PreviousFill:
					if i != 0 {
						vals[j] = results[i-1][j]
					}
				case influxql.NumberFill:
					vals[j] = e.stmt.FillValue
				}
			}
		}
	}
	return results
}

// processDerivative returns the derivatives of the results
func (e *SelectExecutor) processDerivative(results [][]interface{}) [][]interface{} {
	// Return early if we're not supposed to process the derivatives
	if e.stmt.HasDerivative() {
		interval, err := derivativeInterval(e.stmt)
		if err != nil {
			return results // XXX need to handle this better.
		}

		// Determines whether to drop negative differences
		isNonNegative := e.stmt.FunctionCalls()[0].Name == "non_negative_derivative"
		return ProcessAggregateDerivative(results, isNonNegative, interval)
	}
	return results
}

// Close closes the executor such that all resources are released. Once closed,
// an executor may not be re-used.
func (e *SelectExecutor) close() {
	if e != nil {
		for _, m := range e.mappers {
			m.Close()
		}
	}
}

func (e *SelectExecutor) processFunctions(results [][]interface{}, columnNames []string) ([][]interface{}, error) {
	callInPosition := e.stmt.FunctionCallsByPosition()
	hasTimeField := e.stmt.HasTimeFieldSpecified()

	var err error
	for i, calls := range callInPosition {
		// We can only support expanding fields if a single selector call was specified
		// i.e. select tx, max(rx) from foo
		// If you have multiple selectors or aggregates, there is no way of knowing who gets to insert the values, so we don't
		// i.e. select tx, max(rx), min(rx) from foo
		if len(calls) == 1 {
			var c *influxql.Call
			c = calls[0]

			switch c.Name {
			case "top", "bottom":
				results, err = e.processAggregates(results, columnNames, c)
				if err != nil {
					return results, err
				}
			case "first", "last", "min", "max":
				results, err = e.processSelectors(results, i, hasTimeField, columnNames)
				if err != nil {
					return results, err
				}
			}
		}
	}

	return results, nil
}

func (e *SelectExecutor) processSelectors(results [][]interface{}, callPosition int, hasTimeField bool, columnNames []string) ([][]interface{}, error) {
	for i, vals := range results {
		for j := 1; j < len(vals); j++ {
			switch v := vals[j].(type) {
			case PositionPoint:
				tMin := vals[0].(time.Time)
				results[i] = e.selectorPointToQueryResult(vals, hasTimeField, callPosition, v, tMin, columnNames)
			}
		}
	}
	return results, nil
}

func (e *SelectExecutor) selectorPointToQueryResult(row []interface{}, hasTimeField bool, columnIndex int, p PositionPoint, tMin time.Time, columnNames []string) []interface{} {
	// if the row doesn't have enough columns, expand it
	if len(row) != len(columnNames) {
		row = append(row, make([]interface{}, len(columnNames)-len(row))...)
	}
	callCount := len(e.stmt.FunctionCalls())
	if callCount == 1 {
		tm := time.Unix(0, p.Time).UTC().Format(time.RFC3339Nano)
		// If we didn't explicity ask for time, and we have a group by, then use TMIN for the time returned
		if len(e.stmt.Dimensions) > 0 && !hasTimeField {
			tm = tMin.UTC().Format(time.RFC3339Nano)
		}
		row[0] = tm
	}
	for i, c := range columnNames {
		// skip over time, we already handled that above
		if i == 0 {
			continue
		}
		if (i == columnIndex && hasTimeField) || (i == columnIndex+1 && !hasTimeField) {
			row[i] = p.Value
			continue
		}

		if callCount == 1 {
			// Always favor fields over tags if there is a name collision
			if t, ok := p.Fields[c]; ok {
				row[i] = t
			} else if t, ok := p.Tags[c]; ok {
				// look in the tags for a value
				row[i] = t
			}
		}
	}
	return row
}

func (e *SelectExecutor) processAggregates(results [][]interface{}, columnNames []string, call *influxql.Call) ([][]interface{}, error) {
	var values [][]interface{}

	// Check if we have a group by, if not, rewrite the entire result by flattening it out
	for _, vals := range results {
		// start at 1 because the first value is always time
		for j := 1; j < len(vals); j++ {
			switch v := vals[j].(type) {
			case PositionPoints:
				tMin := vals[0].(time.Time)
				for _, p := range v {
					result := e.aggregatePointToQueryResult(p, tMin, call, columnNames)
					values = append(values, result)
				}
			case nil:
				continue
			default:
				return nil, fmt.Errorf("unrechable code - processAggregates for type %T %v", v, v)
			}
		}
	}
	return values, nil
}

func (e *SelectExecutor) aggregatePointToQueryResult(p PositionPoint, tMin time.Time, call *influxql.Call, columnNames []string) []interface{} {
	tm := time.Unix(0, p.Time).UTC().Format(time.RFC3339Nano)
	// If we didn't explicity ask for time, and we have a group by, then use TMIN for the time returned
	if len(e.stmt.Dimensions) > 0 && !e.stmt.HasTimeFieldSpecified() {
		tm = tMin.UTC().Format(time.RFC3339Nano)
	}
	vals := []interface{}{tm}
	for _, c := range columnNames {
		if c == call.Name {
			vals = append(vals, p.Value)
			continue
		}
		// TODO in the future fields will also be available to us.
		// we should always favor fields over tags if there is a name collision

		// look in the tags for a value
		if t, ok := p.Tags[c]; ok {
			vals = append(vals, t)
		}
	}
	return vals
}

// limitedRowWriter accepts raw mapper values, and will emit those values as rows in chunks
// of the given size. If the chunk size is 0, no chunking will be performed. In addiiton if
// limit is reached, outstanding values will be emitted. If limit is zero, no limit is enforced.
type limitedRowWriter struct {
	chunkSize   int
	limit       int
	offset      int
	name        string
	tags        map[string]string
	fields      influxql.Fields
	selectNames []string
	aliasNames  []string
	c           chan *models.Row

	currValues  []*MapperValue
	totalOffSet int
	totalSent   int

	transformer interface {
		Process(input []*MapperValue) []*MapperValue
	}
}

// Add accepts a slice of values, and will emit those values as per chunking requirements.
// If limited is returned as true, the limit was also reached and no more values should be
// added. In that case only up the limit of values are emitted.
func (r *limitedRowWriter) Add(values []*MapperValue) (limited bool) {
	if r.currValues == nil {
		r.currValues = make([]*MapperValue, 0, r.chunkSize)
	}

	// Enforce offset.
	if r.totalOffSet < r.offset {
		// Still some offsetting to do.
		offsetRequired := r.offset - r.totalOffSet
		if offsetRequired >= len(values) {
			r.totalOffSet += len(values)
			return false
		} else {
			// Drop leading values and keep going.
			values = values[offsetRequired:]
			r.totalOffSet += offsetRequired
		}
	}
	r.currValues = append(r.currValues, values...)

	// Check limit.
	limitReached := r.limit > 0 && r.totalSent+len(r.currValues) >= r.limit
	if limitReached {
		// Limit will be satified with current values. Truncate 'em.
		r.currValues = r.currValues[:r.limit-r.totalSent]
	}

	// Is chunking in effect?
	if r.chunkSize != IgnoredChunkSize {
		// Chunking level reached?
		for len(r.currValues) >= r.chunkSize {
			index := len(r.currValues) - (len(r.currValues) - r.chunkSize)
			r.c <- r.processValues(r.currValues[:index])
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
	if r == nil {
		return
	}

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
	r.currValues = nil
}

// processValues emits the given values in a single row.
func (r *limitedRowWriter) processValues(values []*MapperValue) *models.Row {
	defer func() {
		r.totalSent += len(values)
	}()

	selectNames := r.selectNames
	aliasNames := r.aliasNames

	if r.transformer != nil {
		values = r.transformer.Process(values)
	}

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
		aliasNames = append([]string{"time"}, aliasNames...)
	}

	// since selectNames can contain tags, we need to strip them out
	selectFields := make([]string, 0, len(selectNames))
	aliasFields := make([]string, 0, len(selectNames))

	for i, n := range selectNames {
		if _, found := r.tags[n]; !found {
			selectFields = append(selectFields, n)
			aliasFields = append(aliasFields, aliasNames[i])
		}
	}

	row := &models.Row{
		Name:    r.name,
		Tags:    r.tags,
		Columns: aliasFields,
	}

	// Kick out an empty row it no results available.
	if len(values) == 0 {
		return row
	}

	// if they've selected only a single value we have to handle things a little differently
	singleValue := len(selectFields) == SelectColumnCountWithOneValue

	// the results will have all of the raw mapper results, convert into the row
	for _, v := range values {
		vals := make([]interface{}, len(selectFields))

		if singleValue {
			vals[0] = time.Unix(0, v.Time).UTC()
			switch val := v.Value.(type) {
			case map[string]interface{}:
				vals[1] = val[selectFields[1]]
			default:
				vals[1] = val
			}
		} else {
			fields := v.Value.(map[string]interface{})

			// time is always the first value
			vals[0] = time.Unix(0, v.Time).UTC()

			// populate the other values
			for i := 1; i < len(selectFields); i++ {
				f, ok := fields[selectFields[i]]
				if ok {
					vals[i] = f
					continue
				}
				if v.Tags != nil {
					f, ok = v.Tags[selectFields[i]]
					if ok {
						vals[i] = f
					}
				}
			}
		}

		row.Values = append(row.Values, vals)
	}

	// Perform any mathematical post-processing.
	row.Values = processForMath(r.fields, row.Values)

	return row
}

type RawQueryDerivativeProcessor struct {
	LastValueFromPreviousChunk *MapperValue
	IsNonNegative              bool // Whether to drop negative differences
	DerivativeInterval         time.Duration
}

func (rqdp *RawQueryDerivativeProcessor) canProcess(input *MapperValue) bool {
	// Cannot process a nil value
	if input == nil {
		return false
	}

	// See if the field value is numeric, if it's not, we can't process the derivative
	validType := false
	switch input.Value.(type) {
	case int64:
		validType = true
	case float64:
		validType = true
	}

	return validType
}

func (rqdp *RawQueryDerivativeProcessor) Process(input []*MapperValue) []*MapperValue {
	if len(input) == 0 {
		return input
	}

	if len(input) == 1 {
		return []*MapperValue{
			&MapperValue{
				Time:  input[0].Time,
				Value: 0.0,
			},
		}
	}

	if rqdp.LastValueFromPreviousChunk == nil {
		rqdp.LastValueFromPreviousChunk = input[0]
	}

	derivativeValues := []*MapperValue{}
	for i := 1; i < len(input); i++ {
		v := input[i]

		// If we can't use the current or prev value (wrong time, nil), just append
		// nil
		if !rqdp.canProcess(v) || !rqdp.canProcess(rqdp.LastValueFromPreviousChunk) {
			derivativeValues = append(derivativeValues, &MapperValue{
				Time:  v.Time,
				Value: nil,
			})
			continue
		}

		// Calculate the derivative of successive points by dividing the difference
		// of each value by the elapsed time normalized to the interval
		diff := int64toFloat64(v.Value) - int64toFloat64(rqdp.LastValueFromPreviousChunk.Value)

		elapsed := v.Time - rqdp.LastValueFromPreviousChunk.Time

		value := 0.0
		if elapsed > 0 {
			value = diff / (float64(elapsed) / float64(rqdp.DerivativeInterval))
		}

		rqdp.LastValueFromPreviousChunk = v

		// Drop negative values for non-negative derivatives
		if rqdp.IsNonNegative && diff < 0 {
			continue
		}

		derivativeValues = append(derivativeValues, &MapperValue{
			Time:  v.Time,
			Value: value,
		})
	}

	return derivativeValues
}

// processForMath will apply any math that was specified in the select statement
// against the passed in results
func processForMath(fields influxql.Fields, results [][]interface{}) [][]interface{} {
	hasMath := false
	for _, f := range fields {
		if _, ok := f.Expr.(*influxql.BinaryExpr); ok {
			hasMath = true
		} else if _, ok := f.Expr.(*influxql.ParenExpr); ok {
			hasMath = true
		}
	}

	if !hasMath {
		return results
	}

	processors := make([]influxql.Processor, len(fields))
	startIndex := 1
	for i, f := range fields {
		processors[i], startIndex = influxql.GetProcessor(f.Expr, startIndex)
	}

	mathResults := make([][]interface{}, len(results))
	for i, _ := range mathResults {
		mathResults[i] = make([]interface{}, len(fields)+1)
		// put the time in
		mathResults[i][0] = results[i][0]
		for j, p := range processors {
			mathResults[i][j+1] = p(results[i])
		}
	}

	return mathResults
}

// ProcessAggregateDerivative returns the derivatives of an aggregate result set
func ProcessAggregateDerivative(results [][]interface{}, isNonNegative bool, interval time.Duration) [][]interface{} {
	// Return early if we can't calculate derivatives
	if len(results) == 0 {
		return results
	}

	// If we only have 1 value, then the value did not change, so return
	// a single row w/ 0.0
	if len(results) == 1 {
		return [][]interface{}{
			[]interface{}{results[0][0], 0.0},
		}
	}

	// Otherwise calculate the derivatives as the difference between consecutive
	// points divided by the elapsed time.  Then normalize to the requested
	// interval.
	derivatives := [][]interface{}{}
	for i := 1; i < len(results); i++ {
		prev := results[i-1]
		cur := results[i]

		// If current value is nil, append nil for the value
		if prev[1] == nil || cur[1] == nil {
			derivatives = append(derivatives, []interface{}{
				cur[0], nil,
			})
			continue
		}

		// Check the value's type to ensure it's an numeric, if not, return a nil result. We only check the first value
		// because derivatives cannot be combined with other aggregates currently.
		validType := false
		switch cur[1].(type) {
		case int64:
			validType = true
		case float64:
			validType = true
		}

		if !validType {
			derivatives = append(derivatives, []interface{}{
				cur[0], nil,
			})
			continue
		}

		elapsed := cur[0].(time.Time).Sub(prev[0].(time.Time))
		diff := int64toFloat64(cur[1]) - int64toFloat64(prev[1])
		value := 0.0
		if elapsed > 0 {
			value = float64(diff) / (float64(elapsed) / float64(interval))
		}

		// Drop negative values for non-negative derivatives
		if isNonNegative && diff < 0 {
			continue
		}

		val := []interface{}{
			cur[0],
			value,
		}
		derivatives = append(derivatives, val)
	}

	return derivatives
}

// derivativeInterval returns the time interval for the one (and only) derivative func
func derivativeInterval(stmt *influxql.SelectStatement) (time.Duration, error) {
	if len(stmt.FunctionCalls()[0].Args) == 2 {
		return stmt.FunctionCalls()[0].Args[1].(*influxql.DurationLiteral).Val, nil
	}
	interval, err := stmt.GroupByInterval()
	if err != nil {
		return 0, err
	}
	if interval > 0 {
		return interval, nil
	}
	return time.Second, nil
}

// resultsEmpty will return true if the all the result values are empty or contain only nulls
func resultsEmpty(resultValues [][]interface{}) bool {
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

func int64toFloat64(v interface{}) float64 {
	switch v.(type) {
	case int64:
		return float64(v.(int64))
	case float64:
		return v.(float64)
	}
	panic(fmt.Sprintf("expected either int64 or float64, got %v", v))
}

type int64arr []int64

func (a int64arr) Len() int           { return len(a) }
func (a int64arr) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a int64arr) Less(i, j int) bool { return a[i] < a[j] }
