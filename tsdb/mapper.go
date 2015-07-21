package tsdb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/boltdb/bolt"
	"github.com/influxdb/influxdb/influxql"
)

// mapperValue is a complex type, which can encapsulate data from both raw and aggregate
// mappers. This currently allows marshalling and network system to remain simpler. For
// aggregate output Time is ignored, and actual Time-Value pairs are contained soley
// within the Value field.
type mapperValue struct {
	Time  int64       `json:"time,omitempty"`  // Ignored for aggregate output.
	Value interface{} `json:"value,omitempty"` // For aggregate, contains interval time multiple values.
}

type mapperValues []*mapperValue

func (a mapperValues) Len() int           { return len(a) }
func (a mapperValues) Less(i, j int) bool { return a[i].Time < a[j].Time }
func (a mapperValues) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

type MapperOutput struct {
	Name   string            `json:"name,omitempty"`
	Tags   map[string]string `json:"tags,omitempty"`
	Values []*mapperValue    `json:"values,omitempty"` // For aggregates contains a single value at [0]
}

func (mo *MapperOutput) key() string {
	return formMeasurementTagSetKey(mo.Name, mo.Tags)
}

// LocalMapper is for retrieving data for a query, from a given shard.
type LocalMapper struct {
	shard           *Shard
	stmt            influxql.Statement
	selectStmt      *influxql.SelectStatement
	rawMode         bool
	chunkSize       int
	tx              *bolt.Tx        // Read transaction for this shard.
	queryTMin       int64           // Minimum time of the query.
	queryTMax       int64           // Maximum time of the query.
	whereFields     []string        // field names that occur in the where clause
	selectFields    []string        // field names that occur in the select clause
	selectTags      []string        // tag keys that occur in the select clause
	cursors         []*tagSetCursor // Cursors per tag sets.
	currCursorIndex int             // Current tagset cursor being drained.

	// The following attributes are only used when mappers are for aggregate queries.

	queryTMinWindow int64              // Minimum time of the query floored to start of interval.
	intervalSize    int64              // Size of each interval.
	numIntervals    int                // Maximum number of intervals to return.
	currInterval    int                // Current interval for which data is being fetched.
	mapFuncs        []influxql.MapFunc // The mapping functions.
	fieldNames      []string           // the field name being read for mapping.
}

// NewLocalMapper returns a mapper for the given shard, which will return data for the SELECT statement.
func NewLocalMapper(shard *Shard, stmt influxql.Statement, chunkSize int) *LocalMapper {
	m := &LocalMapper{
		shard:     shard,
		stmt:      stmt,
		chunkSize: chunkSize,
		cursors:   make([]*tagSetCursor, 0),
	}

	if s, ok := stmt.(*influxql.SelectStatement); ok {
		m.selectStmt = s
		m.rawMode = (s.IsRawQuery && !s.HasDistinct()) || s.IsSimpleDerivative()
	}
	return m
}

// openMeta opens the mapper for a meta query.
func (lm *LocalMapper) openMeta() error {
	return errors.New("not implemented")
}

// Open opens the local mapper.
func (lm *LocalMapper) Open() error {
	var err error

	// Get a read-only transaction.
	tx, err := lm.shard.DB().Begin(false)
	if err != nil {
		return err
	}
	lm.tx = tx

	if lm.selectStmt == nil {
		return lm.openMeta()
	}

	// Set all time-related parameters on the mapper.
	lm.queryTMin, lm.queryTMax = influxql.TimeRangeAsEpochNano(lm.selectStmt.Condition)

	if !lm.rawMode {
		if err := lm.initializeMapFunctions(); err != nil {
			return err
		}

		// For GROUP BY time queries, limit the number of data points returned by the limit and offset
		d, err := lm.selectStmt.GroupByInterval()
		if err != nil {
			return err
		}
		lm.intervalSize = d.Nanoseconds()
		if lm.queryTMin == 0 || lm.intervalSize == 0 {
			lm.numIntervals = 1
			lm.intervalSize = lm.queryTMax - lm.queryTMin
		} else {
			intervalTop := lm.queryTMax/lm.intervalSize*lm.intervalSize + lm.intervalSize
			intervalBottom := lm.queryTMin / lm.intervalSize * lm.intervalSize
			lm.numIntervals = int((intervalTop - intervalBottom) / lm.intervalSize)
		}

		if lm.selectStmt.Limit > 0 || lm.selectStmt.Offset > 0 {
			// ensure that the offset isn't higher than the number of points we'd get
			if lm.selectStmt.Offset > lm.numIntervals {
				return nil
			}

			// Take the lesser of either the pre computed number of GROUP BY buckets that
			// will be in the result or the limit passed in by the user
			if lm.selectStmt.Limit < lm.numIntervals {
				lm.numIntervals = lm.selectStmt.Limit
			}
		}

		// If we are exceeding our MaxGroupByPoints error out
		if lm.numIntervals > MaxGroupByPoints {
			return errors.New("too many points in the group by interval. maybe you forgot to specify a where time clause?")
		}

		// Ensure that the start time for the results is on the start of the window.
		lm.queryTMinWindow = lm.queryTMin
		if lm.intervalSize > 0 {
			lm.queryTMinWindow = lm.queryTMinWindow / lm.intervalSize * lm.intervalSize
		}
	}

	selectFields := newStringSet()
	selectTags := newStringSet()
	whereFields := newStringSet()

	// Create the TagSet cursors for the Mapper.
	for _, src := range lm.selectStmt.Sources {
		mm, ok := src.(*influxql.Measurement)
		if !ok {
			return fmt.Errorf("invalid source type: %#v", src)
		}

		m := lm.shard.index.Measurement(mm.Name)
		if m == nil {
			// This shard have never received data for the measurement. No Mapper
			// required.
			return nil
		}

		// Validate that ANY GROUP BY is not a field for thie measurement.
		if err := m.ValidateGroupBy(lm.selectStmt); err != nil {
			return err
		}

		// Create tagset cursors and determine various field types within SELECT statement.
		tsf, err := createTagSetsAndFields(m, lm.selectStmt)
		if err != nil {
			return err
		}
		tagSets := tsf.tagSets
		selectFields.add(tsf.selectFields...)
		selectTags.add(tsf.selectTags...)
		whereFields.add(tsf.whereFields...)

		// Validate that any GROUP BY is not on a field
		if err := m.ValidateGroupBy(lm.selectStmt); err != nil {
			return err
		}

		// SLIMIT and SOFFSET the unique series
		if lm.selectStmt.SLimit > 0 || lm.selectStmt.SOffset > 0 {
			if lm.selectStmt.SOffset > len(tagSets) {
				tagSets = nil
			} else {
				if lm.selectStmt.SOffset+lm.selectStmt.SLimit > len(tagSets) {
					lm.selectStmt.SLimit = len(tagSets) - lm.selectStmt.SOffset
				}

				tagSets = tagSets[lm.selectStmt.SOffset : lm.selectStmt.SOffset+lm.selectStmt.SLimit]
			}
		}

		// Create all cursors for reading the data from this shard.
		for _, t := range tagSets {
			cursors := []*seriesCursor{}

			for i, key := range t.SeriesKeys {
				c := createCursorForSeries(lm.tx, lm.shard, key)
				if c == nil {
					// No data exists for this key.
					continue
				}
				cm := newSeriesCursor(c, t.Filters[i])
				cm.SeekTo(lm.queryTMin)
				cursors = append(cursors, cm)
			}
			tsc := newTagSetCursor(m.Name, t.Tags, cursors, lm.shard.FieldCodec(m.Name))
			lm.cursors = append(lm.cursors, tsc)
		}
		sort.Sort(tagSetCursors(lm.cursors))
	}

	lm.selectFields = selectFields.list()
	lm.selectTags = selectTags.list()
	lm.whereFields = whereFields.list()

	// If the query does not aggregate, then at least 1 SELECT field should be present.
	if lm.rawMode && len(lm.selectFields) == 0 {
		// None of the SELECT fields exist in this data. Wipe out all tagset cursors.
		lm.cursors = nil
	}

	return nil
}

func (lm *LocalMapper) NextChunk() (interface{}, error) {
	if lm.rawMode {
		return lm.nextChunkRaw()
	}
	return lm.nextChunkAgg()
}

// nextChunkRaw returns the next chunk of data. Data comes in the same order as the
// tags return by TagSets. A chunk never contains data for more than 1 tagset.
// If there is no more data for any tagset, nil will be returned.
func (lm *LocalMapper) nextChunkRaw() (interface{}, error) {
	var output *MapperOutput
	for {
		if lm.currCursorIndex == len(lm.cursors) {
			// All tagset cursors processed. NextChunk'ing complete.
			return nil, nil
		}
		cursor := lm.cursors[lm.currCursorIndex]

		_, k, v := cursor.Next(lm.queryTMin, lm.queryTMax, lm.selectFields, lm.whereFields)
		if v == nil {
			// Tagset cursor is empty, move to next one.
			lm.currCursorIndex++
			if output != nil {
				// There is data, so return it and continue when next called.
				return output, nil
			} else {
				// Just go straight to the next cursor.
				continue
			}
		}

		if output == nil {
			output = &MapperOutput{
				Name: cursor.measurement,
				Tags: cursor.tags,
			}
		}
		value := &mapperValue{Time: k, Value: v}
		output.Values = append(output.Values, value)
		if len(output.Values) == lm.chunkSize {
			return output, nil
		}
	}
}

// nextChunkAgg returns the next chunk of data, which is the next interval of data
// for the current tagset. Tagsets are always processed in the same order as that
// returned by AvailTagsSets(). When there is no more data for any tagset nil
// is returned.
func (lm *LocalMapper) nextChunkAgg() (interface{}, error) {
	var output *MapperOutput
	for {
		if lm.currCursorIndex == len(lm.cursors) {
			// All tagset cursors processed. NextChunk'ing complete.
			return nil, nil
		}
		cursor := lm.cursors[lm.currCursorIndex]
		tmin, tmax := lm.nextInterval()

		if tmin < 0 {
			// All intervals complete for this tagset. Move to the next tagset.
			lm.currInterval = 0
			lm.currCursorIndex++
			continue
		}

		// Prep the return data for this tagset. This will hold data for a single interval
		// for a single tagset.
		if output == nil {
			output = &MapperOutput{
				Name:   cursor.measurement,
				Tags:   cursor.tags,
				Values: make([]*mapperValue, 1),
			}
			// Aggregate values only use the first entry in the Values field. Set the time
			// to the start of the interval.
			output.Values[0] = &mapperValue{
				Time:  tmin,
				Value: make([]interface{}, 0)}
		}

		// Always clamp tmin. This can happen as bucket-times are bucketed to the nearest
		// interval, and this can be less than the times in the query.
		qmin := tmin
		if qmin < lm.queryTMin {
			qmin = lm.queryTMin
		}

		for i := range lm.mapFuncs {
			// Set the cursor to the start of the interval. This is not ideal, as it should
			// really calculate the values all in 1 pass, but that would require changes
			// to the mapper functions, which can come later.
			cursor.SeekTo(tmin)

			// Wrap the tagset cursor so it implements the mapping functions interface.
			f := func() (seriesKey string, time int64, value interface{}) {
				return cursor.Next(qmin, tmax, []string{lm.fieldNames[i]}, lm.whereFields)
			}

			tagSetCursor := &aggTagSetCursor{
				nextFunc: f,
			}

			// Execute the map function which walks the entire interval, and aggregates
			// the result.
			values := output.Values[0].Value.([]interface{})
			output.Values[0].Value = append(values, lm.mapFuncs[i](tagSetCursor))
		}
		return output, nil
	}
}

// nextInterval returns the next interval for which to return data. If start is less than 0
// there are no more intervals.
func (lm *LocalMapper) nextInterval() (start, end int64) {
	t := lm.queryTMinWindow + int64(lm.currInterval+lm.selectStmt.Offset)*lm.intervalSize

	// Onto next interval.
	lm.currInterval++
	if t > lm.queryTMax || lm.currInterval > lm.numIntervals {
		start, end = -1, 1
	} else {
		start, end = t, t+lm.intervalSize
	}
	return
}

// initializeMapFunctions initialize the mapping functions for the mapper. This only applies
// to aggregate queries.
func (lm *LocalMapper) initializeMapFunctions() error {
	var err error
	// Set up each mapping function for this statement.
	aggregates := lm.selectStmt.FunctionCalls()
	lm.mapFuncs = make([]influxql.MapFunc, len(aggregates))
	lm.fieldNames = make([]string, len(lm.mapFuncs))
	for i, c := range aggregates {
		lm.mapFuncs[i], err = influxql.InitializeMapFunc(c)
		if err != nil {
			return err
		}

		// Check for calls like `derivative(lmean(value), 1d)`
		var nested *influxql.Call = c
		if fn, ok := c.Args[0].(*influxql.Call); ok {
			nested = fn
		}
		switch lit := nested.Args[0].(type) {
		case *influxql.VarRef:
			lm.fieldNames[i] = lit.Val
		case *influxql.Distinct:
			if c.Name != "count" {
				return fmt.Errorf("aggregate call didn't contain a field %s", c.String())
			}
			lm.fieldNames[i] = lit.Val
		default:
			return fmt.Errorf("aggregate call didn't contain a field %s", c.String())
		}
	}

	return nil
}

// TagSets returns the list of TagSets for which this mapper has data.
func (lm *LocalMapper) TagSets() []string {
	return tagSetCursors(lm.cursors).Keys()
}

// Close closes the mapper.
func (lm *LocalMapper) Close() {
	if lm != nil && lm.tx != nil {
		_ = lm.tx.Rollback()
	}
}

// aggTagSetCursor wraps a standard tagSetCursor, such that the values it emits are aggregated
// by intervals.
type aggTagSetCursor struct {
	nextFunc func() (seriesKey string, time int64, value interface{})
}

// Next returns the next value for the aggTagSetCursor. It implements the interface expected
// by the mapping functions.
func (a *aggTagSetCursor) Next() (seriesKey string, time int64, value interface{}) {
	return a.nextFunc()
}

// tagSetCursor is virtual cursor that iterates over mutiple series cursors, as though it were
// a single series.
type tagSetCursor struct {
	measurement string            // Measurement name
	tags        map[string]string // Tag key-value pairs
	cursors     []*seriesCursor   // Underlying series cursors.
	decoder     *FieldCodec       // decoder for the raw data bytes
}

// tagSetCursors represents a sortable slice of tagSetCursors.
type tagSetCursors []*tagSetCursor

func (a tagSetCursors) Len() int           { return len(a) }
func (a tagSetCursors) Less(i, j int) bool { return a[i].key() < a[j].key() }
func (a tagSetCursors) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

func (a tagSetCursors) Keys() []string {
	keys := []string{}
	for i := range a {
		keys = append(keys, a[i].key())
	}
	sort.Strings(keys)
	return keys
}

// newTagSetCursor returns a tagSetCursor
func newTagSetCursor(m string, t map[string]string, c []*seriesCursor, d *FieldCodec) *tagSetCursor {
	return &tagSetCursor{
		measurement: m,
		tags:        t,
		cursors:     c,
		decoder:     d,
	}
}

func (tsc *tagSetCursor) key() string {
	return formMeasurementTagSetKey(tsc.measurement, tsc.tags)
}

// Next returns the next matching series-key, timestamp and byte slice for the tagset. Filtering
// is enforced on the values. If there is no matching value, then a nil result is returned.
func (tsc *tagSetCursor) Next(tmin, tmax int64, selectFields, whereFields []string) (string, int64, interface{}) {
	for {
		// Find the cursor with the lowest timestamp, as that is the one to be read next.
		minCursor := tsc.nextCursor(tmin, tmax)
		if minCursor == nil {
			// No cursor of this tagset has any matching data.
			return "", 0, nil
		}
		timestamp, bytes := minCursor.Next()

		var value interface{}
		if len(selectFields) > 1 {
			if fieldsWithNames, err := tsc.decoder.DecodeFieldsWithNames(bytes); err == nil {
				value = fieldsWithNames

				// if there's a where clause, make sure we don't need to filter this value
				if minCursor.filter != nil && !matchesWhere(minCursor.filter, fieldsWithNames) {
					value = nil
				}
			}
		} else {
			// With only 1 field SELECTed, decoding all fields may be avoidable, which is faster.
			var err error
			value, err = tsc.decoder.DecodeByName(selectFields[0], bytes)
			if err != nil {
				continue
			}

			// If there's a WHERE clase, see if we need to filter
			if minCursor.filter != nil {
				// See if the WHERE is only on this field or on one or more other fields.
				// If the latter, we'll have to decode everything
				if len(whereFields) == 1 && whereFields[0] == selectFields[0] {
					if !matchesWhere(minCursor.filter, map[string]interface{}{selectFields[0]: value}) {
						value = nil
					}
				} else { // Decode everything
					fieldsWithNames, err := tsc.decoder.DecodeFieldsWithNames(bytes)
					if err != nil || !matchesWhere(minCursor.filter, fieldsWithNames) {
						value = nil
					}
				}
			}
		}

		// Value didn't match, look for the next one.
		if value == nil {
			continue
		}

		return "", timestamp, value
	}
}

// SeekTo seeks each underlying cursor to the specified key.
func (tsc *tagSetCursor) SeekTo(key int64) {
	for _, c := range tsc.cursors {
		c.SeekTo(key)
	}
}

// IsEmpty returns whether the tagsetCursor has any more data for the given interval.
func (tsc *tagSetCursor) IsEmptyForInterval(tmin, tmax int64) bool {
	for _, c := range tsc.cursors {
		k, _ := c.Peek()
		if k != 0 && k >= tmin && k <= tmax {
			return false
		}
	}
	return true
}

// nextCursor returns the series cursor with the lowest next timestamp, within in the specified
// range. If none exists, nil is returned.
func (tsc *tagSetCursor) nextCursor(tmin, tmax int64) *seriesCursor {
	var minCursor *seriesCursor
	var timestamp int64
	for _, c := range tsc.cursors {
		timestamp, _ = c.Peek()
		if timestamp != 0 && ((timestamp == tmin) || (timestamp >= tmin && timestamp < tmax)) {
			if minCursor == nil {
				minCursor = c
			} else {
				if currMinTimestamp, _ := minCursor.Peek(); timestamp < currMinTimestamp {
					minCursor = c
				}
			}
		}
	}
	return minCursor
}

// seriesCursor is a cursor that walks a single series. It provides lookahead functionality.
type seriesCursor struct {
	cursor      *shardCursor // BoltDB cursor for a series
	filter      influxql.Expr
	keyBuffer   int64  // The current timestamp key for the cursor
	valueBuffer []byte // The current value for the cursor
}

// newSeriesCursor returns a new instance of a series cursor.
func newSeriesCursor(b *shardCursor, filter influxql.Expr) *seriesCursor {
	return &seriesCursor{
		cursor:    b,
		filter:    filter,
		keyBuffer: -1, // Nothing buffered.
	}
}

// Peek returns the next timestamp and value, without changing what will be
// be returned by a call to Next()
func (mc *seriesCursor) Peek() (key int64, value []byte) {
	if mc.keyBuffer == -1 {
		k, v := mc.cursor.Next()
		if k == nil {
			mc.keyBuffer = 0
		} else {
			mc.keyBuffer = int64(btou64(k))
			mc.valueBuffer = v
		}
	}

	key, value = mc.keyBuffer, mc.valueBuffer
	return
}

// SeekTo positions the cursor at the key, such that Next() will return
// the key and value at key.
func (mc *seriesCursor) SeekTo(key int64) {
	k, v := mc.cursor.Seek(u64tob(uint64(key)))
	if k == nil {
		mc.keyBuffer = 0
	} else {
		mc.keyBuffer, mc.valueBuffer = int64(btou64(k)), v
	}
}

// Next returns the next timestamp and value from the cursor.
func (mc *seriesCursor) Next() (key int64, value []byte) {
	if mc.keyBuffer != -1 {
		key, value = mc.keyBuffer, mc.valueBuffer
		mc.keyBuffer, mc.valueBuffer = -1, nil
	} else {
		k, v := mc.cursor.Next()
		if k == nil {
			key = 0
		} else {
			key, value = int64(btou64(k)), v
		}
	}
	return
}

// createCursorForSeries creates a cursor for walking the given series key. The cursor
// consolidates both the Bolt store and any WAL cache.
func createCursorForSeries(tx *bolt.Tx, shard *Shard, key string) *shardCursor {
	// Retrieve key bucket.
	b := tx.Bucket([]byte(key))

	// Ignore if there is no bucket or points in the cache.
	partitionID := WALPartition([]byte(key))
	if b == nil && len(shard.cache[partitionID][key]) == 0 {
		return nil
	}

	// Retrieve a copy of the in-cache points for the key.
	cache := make([][]byte, len(shard.cache[partitionID][key]))
	copy(cache, shard.cache[partitionID][key])

	// Build a cursor that merges the bucket and cache together.
	cur := &shardCursor{cache: cache}
	if b != nil {
		cur.cursor = b.Cursor()
	}
	return cur
}

type tagSetsAndFields struct {
	tagSets      []*influxql.TagSet
	selectFields []string
	selectTags   []string
	whereFields  []string
}

// createTagSetsAndFields returns the tagsets and various fields given a measurement and
// SELECT statement.
func createTagSetsAndFields(m *Measurement, stmt *influxql.SelectStatement) (*tagSetsAndFields, error) {
	_, tagKeys, err := stmt.Dimensions.Normalize()
	if err != nil {
		return nil, err
	}

	sfs := newStringSet()
	sts := newStringSet()
	wfs := newStringSet()

	// Validate the fields and tags asked for exist and keep track of which are in the select vs the where
	for _, n := range stmt.NamesInSelect() {
		if m.HasField(n) {
			sfs.add(n)
			continue
		}
		if m.HasTagKey(n) {
			sts.add(n)
			tagKeys = append(tagKeys, n)
		}
	}
	for _, n := range stmt.NamesInWhere() {
		if n == "time" {
			continue
		}
		if m.HasField(n) {
			wfs.add(n)
			continue
		}
	}

	// Get the sorted unique tag sets for this statement.
	tagSets, err := m.TagSets(stmt, tagKeys)
	if err != nil {
		return nil, err
	}

	return &tagSetsAndFields{
		tagSets:      tagSets,
		selectFields: sfs.list(),
		selectTags:   sts.list(),
		whereFields:  wfs.list(),
	}, nil
}

// matchesFilter returns true if the value matches the where clause
func matchesWhere(f influxql.Expr, fields map[string]interface{}) bool {
	if ok, _ := influxql.Eval(f, fields).(bool); !ok {
		return false
	}
	return true
}

func formMeasurementTagSetKey(name string, tags map[string]string) string {
	if len(tags) == 0 {
		return name
	}
	return strings.Join([]string{name, string(marshalTags(tags))}, "|")
}

// btou64 converts an 8-byte slice into an uint64.
func btou64(b []byte) uint64 { return binary.BigEndian.Uint64(b) }
