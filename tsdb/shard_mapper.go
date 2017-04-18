package tsdb

import (
	"fmt"
	"math"
	"time"

	"github.com/boltdb/bolt"
	"github.com/influxdb/influxdb/influxql"
)

type ShardMapper interface {
	// Open will open the necessary resources to being the map job. Could be connections to remote servers or
	// hitting the local bolt store
	Open() error

	// Close will close the mapper (either the bolt transaction or the request)
	Close()

	// Begin will set up the mapper to run the map function for a given aggregate call starting at the passed in time.
	Begin(aggregate *influxql.Call, startingTime int64, limit int) error

	// NextInterval will get the time ordered next interval of the given interval size from the mapper.
	NextInterval() (interface{}, error)
}

type LocalShardMultiMapper struct {
	shard     *Shard
	shMappers []*LocalShardMapper
}

func NewLocalShardMultiMapper(shard *Shard) *LocalShardMultiMapper {
	return &LocalShardMultiMapper{
		shard:     shard,
		shMappers: make([]*LocalShardMapper, 0),
	}
}

func (l *LocalShardMultiMapper) Open(stmt *influxql.SelectStatement) error {
	for _, src := range stmt.Sources {
		mm, ok := src.(*influxql.Measurement)
		if !ok {
			return fmt.Errorf("invalid source type: %#v", src)
		}

		m := l.shard.index.Measurement(mm.Name)
		if m == nil {
			return ErrMeasurementNotFound(influxql.QuoteIdent([]string{mm.Database, "", mm.Name}...))
		}

		// Validate the fields and tags asked for exist and keep track of which are in the select vs the where
		var selectFields []string
		var whereFields []string
		var selectTags []string

		for _, n := range stmt.NamesInSelect() {
			if m.HasField(n) {
				selectFields = append(selectFields, n)
				continue
			}
			if !m.HasTagKey(n) {
				return fmt.Errorf("unknown field or tag name in select clause: %s", n)
			}
			selectTags = append(selectTags, n)
		}
		for _, n := range stmt.NamesInWhere() {
			if n == "time" {
				continue
			}
			if m.HasField(n) {
				whereFields = append(whereFields, n)
				continue
			}
			if !m.HasTagKey(n) {
				return fmt.Errorf("unknown field or tag name in where clause: %s", n)
			}
		}

		if len(selectFields) == 0 && len(stmt.FunctionCalls()) == 0 {
			return fmt.Errorf("select statement must include at least one field or function call")
		}

		// Validate that group by is not a field
		for _, d := range stmt.Dimensions {
			switch e := d.Expr.(type) {
			case *influxql.VarRef:
				if !m.HasTagKey(e.Val) {
					return fmt.Errorf("can not use field in group by clause: %s", e.Val)
				}
			}
		}

		// Grab time range from statement.
		tmin, tmax := influxql.TimeRange(stmt.Condition)
		if tmax.IsZero() {
			tmax = time.Now()
		}
		if tmin.IsZero() {
			tmin = time.Unix(0, 0)
		}

		// Determine the interval and GROUP BY tag keys.
		interval, tagKeys, err := stmt.Dimensions.Normalize()
		if err != nil {
			return err
		}

		// get the sorted unique tag sets for this query.
		tagSets, err := m.TagSets(stmt, tagKeys)
		if err != nil {
			return err
		}

		// get the codec for this measuremnt. If this is nil it just means this measurement was
		// never written into this shard, so we can skip it and continue.
		codec := l.shard.FieldCodec(m.Name)
		if codec == nil {
			continue
		}

		for _, t := range tagSets {
			// make a shard mapper for each tagset
			mapper := &LocalShardMapper{
				seriesKeys:      t.SeriesKeys,
				db:              l.shard.DB(),
				decoder:         codec,
				filters:         t.Filters,
				whereFields:     whereFields,
				selectFields:    selectFields,
				selectTags:      selectTags,
				measurementName: m.Name,
				queryTmin:       tmin.UnixNano(),
				queryTmax:       tmax.UnixNano(),
				tmin:            tmin.UnixNano(),
				tmax:            tmax.UnixNano(),
				interval:        interval.Nanoseconds(),
				// multiple mappers may need to be merged together to get the results
				// for a raw query. So each mapper will have to read at least the
				// limit plus the offset in data points to ensure we've hit our mark
				limit: uint64(stmt.Limit) + uint64(stmt.Offset),
			}
			l.shMappers = append(l.shMappers, mapper)
		}
	}

	return nil
}

func (l *LocalShardMultiMapper) Close() {
	for _, sh := range l.shMappers {
		sh.Close()
	}
	return
}

func (l *LocalShardMultiMapper) Begin(aggregate *influxql.Call, startingTime int64, limit int) error {
	for _, sh := range l.shMappers {
		if err := sh.Begin(aggregate, startingTime, limit); err != nil {
			return err
		}
	}
	return nil
}

func (l *LocalShardMultiMapper) NextInterval() (interface{}, error) {
	return nil, nil
}

type LocalShardMapper struct {
	cursorsEmpty     bool             // boolean that lets us know if the cursors are empty
	decoder          *FieldCodec      // decoder for the raw data bytes
	filters          []influxql.Expr  // filters for each series
	cursors          []*bolt.Cursor   // bolt cursors for each series id
	seriesKeys       []string         // seriesKeys to be read from this shard
	db               *bolt.DB         // bolt store for the shard accessed by this mapper
	txn              *bolt.Tx         // read transaction
	mapFunc          influxql.MapFunc // the map func
	fieldID          uint8            // the field ID associated with the mapFunc curently being run
	fieldName        string           // the field name associated with the mapFunc currently being run
	keyBuffer        []int64          // the current timestamp key for each cursor
	valueBuffer      [][]byte         // the current value for each cursor
	measurementName  string           // source measurement specified in the query
	queryTmin        int64            // minimum time specified in the query
	queryTmax        int64            // maximum time specified in the query
	tmin             int64            // the min of the current group by interval being iterated over
	tmax             int64            // the max of the current group by interval being iterated over
	additionalNames  []string         // additional field or tag names that might be requested from the map function
	whereFields      []string         // field names that occur in the where clause
	selectFields     []string         // field names that occur in the select clause
	selectTags       []string         // tag keys that occur in the select clause
	isRaw            bool             // if the query is a non-aggregate query
	interval         int64            // the group by interval of the query, if any
	limit            uint64           // used for raw queries for LIMIT
	perIntervalLimit int              // used for raw queries to determine how far into a chunk we are
	chunkSize        int              // used for raw queries to determine how much data to read before flushing to client
}

// Open opens the LocalShardMapper.
func (l *LocalShardMapper) Open() error {
	// Open the data store
	txn, err := l.db.Begin(false)
	if err != nil {
		return err
	}
	l.txn = txn

	// create a bolt cursor for each unique series id
	l.cursors = make([]*bolt.Cursor, len(l.seriesKeys))

	for i, key := range l.seriesKeys {
		b := l.txn.Bucket([]byte(key))
		if b == nil {
			continue
		}

		l.cursors[i] = b.Cursor()
	}

	return nil
}

// Close closes the LocalShardMapper.
func (l *LocalShardMapper) Close() {
	_ = l.txn.Rollback()
}

// Begin will set up the mapper to run the map function for a given aggregate call starting at the passed in time
func (l *LocalShardMapper) Begin(c *influxql.Call, startingTime int64, chunkSize int) error {
	// set up the buffers. These ensure that we return data in time order
	mapFunc, err := influxql.InitializeMapFunc(c)
	if err != nil {
		return err
	}
	l.mapFunc = mapFunc
	l.keyBuffer = make([]int64, len(l.cursors))
	l.valueBuffer = make([][]byte, len(l.cursors))
	l.chunkSize = chunkSize
	l.tmin = startingTime

	var isCountDistinct bool

	// Determine if this is a raw data query with a single field, multiple fields, or an aggregate
	var fieldName string
	if c == nil { // It's a raw data query
		l.isRaw = true
		if len(l.selectFields) == 1 {
			fieldName = l.selectFields[0]
		}

		// If they haven't set a limit, just set it to the max int size
		if l.limit == 0 {
			l.limit = math.MaxUint64
		}
	} else {
		// Check for calls like `derivative(mean(value), 1d)`
		var nested *influxql.Call = c
		if fn, ok := c.Args[0].(*influxql.Call); ok {
			nested = fn
		}

		switch lit := nested.Args[0].(type) {
		case *influxql.VarRef:
			fieldName = lit.Val
		case *influxql.Distinct:
			if c.Name != "count" {
				return fmt.Errorf("aggregate call didn't contain a field %s", c.String())
			}
			isCountDistinct = true
			fieldName = lit.Val
		default:
			return fmt.Errorf("aggregate call didn't contain a field %s", c.String())
		}

		isCountDistinct = isCountDistinct || (c.Name == "count" && nested.Name == "distinct")
	}

	// set up the field info if a specific field was set for this mapper
	if fieldName != "" {
		fid, err := l.decoder.FieldIDByName(fieldName)
		if err != nil {
			switch {
			case c != nil && c.Name == "distinct":
				return fmt.Errorf(`%s isn't a field on measurement %s; to query the unique values for a tag use SHOW TAG VALUES FROM %[2]s WITH KEY = "%[1]s`, fieldName, l.measurementName)
			case isCountDistinct:
				return fmt.Errorf("%s isn't a field on measurement %s; count(distinct) on tags isn't yet supported", fieldName, l.measurementName)
			}
		}
		l.fieldID = fid
		l.fieldName = fieldName
	}

	// seek the bolt cursors and fill the buffers
	for i, c := range l.cursors {
		// this series may have never been written in this shard group (time range) so the cursor would be nil
		if c == nil {
			l.keyBuffer[i] = 0
			l.valueBuffer[i] = nil
			continue
		}
		k, v := c.Seek(u64tob(uint64(l.queryTmin)))
		if k == nil {
			l.keyBuffer[i] = 0
			l.valueBuffer[i] = nil
			continue
		}
		l.cursorsEmpty = false
		t := int64(btou64(k))
		l.keyBuffer[i] = t
		l.valueBuffer[i] = v
	}
	return nil
}

// NextInterval will get the time ordered next interval of the given interval size from the mapper.
// This is a forward only operation from the start time passed into Begin. Will return nil when
// there is no more data to be read. If this is a raw query, interval should be the max time to
// hit in the query
func (l *LocalShardMapper) NextInterval() (interface{}, error) {
	if l.cursorsEmpty || l.tmin > l.queryTmax {
		return nil, nil
	}

	// After we call to the mapper, this will be the tmin for the next interval.
	nextMin := l.tmin + l.interval

	// Set the upper bound of the interval.
	if l.isRaw {
		l.perIntervalLimit = l.chunkSize
	} else if l.interval > 0 {
		// Set tmax to ensure that the interval lands on the boundary of the interval.
		if l.tmin%l.interval != 0 {
			// The first interval in a query with a group by may be smaller than
			// the others. This happens when they have a where time > clause that
			// is in the middle of the bucket that the group by time creates. That
			// will be the case on the first interval when the tmin % the interval
			// isn't equal to zero.
			nextMin = l.tmin/l.interval*l.interval + l.interval
		}
		l.tmax = nextMin - 1
	}

	// Execute the map function. This local mapper acts as the iterator.
	val := l.mapFunc(l)

	// see if all the cursors are empty
	l.cursorsEmpty = true
	for _, k := range l.keyBuffer {
		if k != 0 {
			l.cursorsEmpty = false
			break
		}
	}

	// Move the interval forward if it's not a raw query. For raw queries we use the limit
	// to advance intervals.
	if !l.isRaw {
		l.tmin = nextMin
	}

	return val, nil
}

// Next returns the next matching timestamped value for the LocalMapper.
func (l *LocalShardMapper) Next() (seriesKey string, timestamp int64, value interface{}) {
	for {
		// If it's a raw query and we've hit the limit of the number of points to read in
		// for either this chunk or for the absolute query, bail.
		if l.isRaw && (l.limit == 0 || l.perIntervalLimit == 0) {
			return "", int64(0), nil
		}

		// Find the minimum timestamp.
		min := -1
		minKey := int64(math.MaxInt64)
		for i, k := range l.keyBuffer {
			if k != 0 && k <= l.tmax && k < minKey && k >= l.tmin {
				min = i
				minKey = k
			}
		}

		// return if there is no more data in this group by interval
		if min == -1 {
			return "", 0, nil
		}

		// Set the current timestamp and seriesID.
		timestamp = l.keyBuffer[min]
		seriesKey = l.seriesKeys[min]

		// Decode either the value, or values we need. Also filter if necessary.
		var value interface{}
		var err error
		if l.isRaw && len(l.selectFields) > 1 {
			if fieldsWithNames, err := l.decoder.DecodeFieldsWithNames(l.valueBuffer[min]); err == nil {
				value = fieldsWithNames

				// If there's a where clause, make sure we don't need to filter this value.
				if l.filters[min] != nil {
					if !matchesWhere(l.filters[min], fieldsWithNames) {
						value = nil
					}
				}
			}
		} else {
			value, err = l.decoder.DecodeByID(l.fieldID, l.valueBuffer[min])

			// If there's a where clasue, see if we need to filter.
			if l.filters[min] != nil {
				// See if the where is only on this field or on one or more other fields. Ff the latter,
				// we'll have to decode everything.
				if len(l.whereFields) == 1 && l.whereFields[0] == l.fieldName {
					if !matchesWhere(l.filters[min], map[string]interface{}{l.fieldName: value}) {
						value = nil
					}
				} else { // Decode everything.
					fieldsWithNames, err := l.decoder.DecodeFieldsWithNames(l.valueBuffer[min])
					if err != nil || !matchesWhere(l.filters[min], fieldsWithNames) {
						value = nil
					}
				}
			}
		}

		// Advance the cursor.
		nextKey, nextVal := l.cursors[min].Next()
		if nextKey == nil {
			l.keyBuffer[min] = 0
		} else {
			l.keyBuffer[min] = int64(btou64(nextKey))
		}
		l.valueBuffer[min] = nextVal

		// If the value didn't match our filter or if we didn't find the field keep iterating.
		if err != nil || value == nil {
			continue
		}

		// If it's a raw query, we always limit the amount we read in.
		if l.isRaw {
			l.limit--
			l.perIntervalLimit--
		}

		return seriesKey, timestamp, value
	}
}

// IsEmpty returns true if either all cursors are nil or all cursors are past the passed in max time.
func (l *LocalShardMapper) IsEmpty(tmax int64) bool {
	if l.cursorsEmpty || l.limit == 0 {
		return true
	}

	// Look at the next time for each cursor.
	for _, t := range l.keyBuffer {
		// If the time is less than the max, we haven't emptied this mapper yet.
		if t != 0 && t <= tmax {
			return false
		}
	}

	return true
}
