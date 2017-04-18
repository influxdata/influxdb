package tsdb

import (
	"fmt"
	"sort"
	"time"

	"github.com/boltdb/bolt"
	"github.com/influxdb/influxdb/influxql"
)

type ShardMapper struct {
	shard *Shard

	tx               *bolt.Tx         // Read transaction for this shard.
	decoder          *FieldCodec      // decoder for the raw data bytes
	cursors          tagSetCursors    // Cursors per tag sets.
	currTagSetCursor int              // Current tagset cursor being iterated.
	mapFunc          influxql.MapFunc // the map func

	whereFields  []string // field names that occur in the where clause
	selectFields []string // field names that occur in the select clause
	selectTags   []string // tag keys that occur in the select clause
	fieldName    string   // the field name associated with the mapFunc currently being run

	queryTMin int64         // the min specified by the query
	queryTMax int64         // the max specified by the query
	interval  time.Duration // GROUP BY interval.
	isRaw     bool          // if the query is a non-aggregate query

	// Iterator state
	currTmin int64 // the min of the current GROUP BY interval being iterated over
	currTmax int64 // the max of the current GROUP BY interval being iterated over
}

func NewShardMapper(shard *Shard) *ShardMapper {
	return &ShardMapper{
		shard:   shard,
		cursors: make([]*tagSetCursor, 0),
	}
}

func (sm *ShardMapper) Open() error {
	tx, err := sm.shard.DB().Begin(false)
	if err != nil {
		return err
	}
	sm.tx = tx

	return nil
}

func (sm *ShardMapper) Close() {
	if sm.tx != nil {
		_ = sm.tx.Rollback()
	}
}

func (sm *ShardMapper) Begin(stmt *influxql.SelectStatement, chunkSize int) error {
	var err error
	if len(stmt.Sources) != 1 {
		return fmt.Errorf("only 1 source per statement supported")
	}
	src := stmt.Sources[0]

	mm := src.(*influxql.Measurement)

	m := sm.shard.index.Measurement(mm.Name)
	if m == nil {
		// This shard have never received data for the measurement. No Mapper
		// required.
		return nil
	}

	// Get a decoder for the measurement.
	sm.decoder = sm.shard.FieldCodec(m.Name)

	// Set all time-related paremeters on the mapper.
	tmin, tmax := influxql.TimeRange(stmt.Condition)
	if tmin.IsZero() {
		sm.queryTMin = time.Unix(0, 0).UnixNano()
	} else {
		sm.queryTMin = tmin.UnixNano()
	}
	if tmax.IsZero() {
		sm.queryTMax = time.Now().UnixNano()
	} else {
		sm.queryTMax = tmax.UnixNano()
	}

	// Set first interval to query range.
	sm.currTmin = sm.queryTMin
	sm.currTmax = sm.queryTMax
	if sm.interval, err = stmt.GroupByInterval(); err != nil {
		return err
	}

	// Validate the fields and tags asked for exist and keep track of which are in the select vs the where
	for _, n := range stmt.NamesInSelect() {
		if m.HasField(n) {
			sm.selectFields = append(sm.selectFields, n)
			continue
		}
		if !m.HasTagKey(n) {
			return fmt.Errorf("unknown field or tag name in select clause: %s", n)
		}
		sm.selectTags = append(sm.selectTags, n)
	}
	for _, n := range stmt.NamesInWhere() {
		if n == "time" {
			continue
		}
		if m.HasField(n) {
			sm.whereFields = append(sm.whereFields, n)
			continue
		}
		if !m.HasTagKey(n) {
			return fmt.Errorf("unknown field or tag name in where clause: %s", n)
		}
	}

	if len(sm.selectFields) == 0 && len(stmt.FunctionCalls()) == 0 {
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

	// Hardcode a raw function.
	sm.mapFunc = influxql.MapRawQuery
	sm.isRaw = true
	sm.fieldName = sm.selectFields[0]

	// Determine the interval and GROUP BY tag keys.
	_, tagKeys, err := stmt.Dimensions.Normalize()
	if err != nil {
		return err
	}

	// Get the sorted unique tag sets for this statement.
	tagSets, err := m.TagSets(stmt, tagKeys)
	if err != nil {
		return err
	}

	// Create all cursors for reading the data from this shard.
	for _, t := range tagSets {
		cursors := []*seriesCursor{}

		for i, key := range t.SeriesKeys {
			c := sm.createCursorForSeries(key)
			if c == nil {
				// No data exists for this key.
				continue
			}
			cm := newSeriesCursor(c, t.Filters[i])
			cm.Seek(sm.queryTMin)
			cursors = append(cursors, cm)
		}
		sm.cursors = append(sm.cursors, newTagSetCursor(m.Name+string(t.Key), cursors, sm))
	}

	// Sort the tag-set cursors such that they are always iterated in the same order.
	sort.Sort(sm.cursors)

	return nil
}

// NextChunk returns the next chunk (interval for now) of data for given tagset. If result is nil
// then the mapper has no more data.
func (sm *ShardMapper) NextChunk() (tagSet string, result interface{}, interval int, err error) {
	// XXX might be overkill, an inefficient.
	if sm.IsEmpty(sm.currTmax) || sm.currTmin > sm.queryTMax {
		return "", nil, 1, nil
	}

	for {
		if sm.currTagSetCursor == len(sm.cursors) {
			// All tag set cursors for this mapper are drained, for this interval.
			return "", nil, 1, nil
		}
		cursor := sm.cursors[sm.currTagSetCursor]
		val := sm.mapFunc(cursor)

		if cursor.IsEmptyForInterval() {
			sm.currTagSetCursor++
		}
		return cursor.key, val, 1, nil
	}

	// Get the current set of series cursors.

	// Worked the all cursor sets for this interval. XXX reset and do next interval.

	// if it's a raw query and we've hit the limit of the number of points to read in
	// for either this chunk or for the absolute query, bail
	//if l.isRaw && (l.limit == 0 || l.perIntervalLimit == 0) {
	//	return "", int64(0), nil
	//}

	// No cursor of this tagset has any matching data.

}

func (sm *ShardMapper) createCursorForSeries(key string) *shardCursor {
	// Retrieve key bucket.
	b := sm.tx.Bucket([]byte(key))

	// Ignore if there is no bucket or points in the cache.
	partitionID := WALPartition([]byte(key))
	if b == nil && len(sm.shard.cache[partitionID][key]) == 0 {
		return nil
	}

	// Retrieve a copy of the in-cache points for the key.
	cache := make([][]byte, len(sm.shard.cache[partitionID][key]))
	copy(cache, sm.shard.cache[partitionID][key])

	// Build a cursor that merges the bucket and cache together.
	cur := &shardCursor{cache: cache}
	if b != nil {
		cur.cursor = b.Cursor()
	}
	return cur
}

// IsEmpty returns true if either all cursors are nil or all cursors are past the given time.
func (sm *ShardMapper) IsEmpty(tmax int64) bool {
	return false
}

// tagSetCursor is virtual cursor that iterates over mutiple series cursors, as though it were
// a single series.
type tagSetCursor struct {
	key     string          // Measurement and tag key-values.
	cursors []*seriesCursor // Cursors per tag sets.
	sm      *ShardMapper    // The parent shard mapper.
}

// tagSetCursors represents a sortable slice of tagSetCursors.
type tagSetCursors []*tagSetCursor

func (a tagSetCursors) Len() int           { return len(a) }
func (a tagSetCursors) Less(i, j int) bool { return a[i].key < a[j].key }
func (a tagSetCursors) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

// newTagSetCursor returns a tagSetCursor
func newTagSetCursor(key string, cursors []*seriesCursor, sm *ShardMapper) *tagSetCursor {
	return &tagSetCursor{
		key:     key,
		cursors: cursors,
		sm:      sm,
	}
}

// Next returns the next matching series-key, timestamp and value for the tagset.
func (tsc *tagSetCursor) Next() (string, int64, interface{}) {
	for {
		// TODO: Enforce a limit, and per-interval limit, check here.

		// Find the cursor with the lowest timestamp, as that is the one to be read next.
		var minCursor *seriesCursor
		for _, c := range tsc.cursors {
			timestamp, _ := c.Peek()
			if timestamp != 0 && timestamp >= tsc.sm.currTmin && timestamp <= tsc.sm.currTmax {
				if minCursor == nil {
					minCursor = c
				} else {
					if t, _ := minCursor.Peek(); timestamp < t {
						minCursor = c
					}
				}
			}
		}

		if minCursor == nil {
			// No cursor of this tagset has any matching data.
			return "", 0, nil
		}

		// We've got the next series to read from.
		timestamp, val := minCursor.Next()

		// Decode either the value, or values we need. Also filter if necessary
		var err error
		var value interface{}
		if tsc.sm.isRaw && len(tsc.sm.selectFields) > 1 {
			if fieldsWithNames, err := tsc.sm.decoder.DecodeFieldsWithNames(val); err == nil {
				value = fieldsWithNames

				// if there's a where clause, make sure we don't need to filter this value
				if minCursor.filter != nil {
					if !matchesWhere(minCursor.filter, fieldsWithNames) {
						value = nil
					}
				}
			}
		} else {
			// In this case a more efficient decode may be available.
			value, err = tsc.sm.decoder.DecodeByName(tsc.sm.fieldName, val)

			// if there's a where clase, see if we need to filter
			if minCursor.filter != nil {
				// see if the where is only on this field or on one or more other fields. if the latter, we'll have to decode everything
				if len(tsc.sm.whereFields) == 1 && tsc.sm.whereFields[0] == tsc.sm.fieldName {
					if !matchesWhere(minCursor.filter, map[string]interface{}{tsc.sm.fieldName: value}) {
						value = nil
					}
				} else { // decode everything
					fieldsWithNames, err := tsc.sm.decoder.DecodeFieldsWithNames(val)
					if err != nil || !matchesWhere(minCursor.filter, fieldsWithNames) {
						value = nil
					}
				}
			}
		}

		// if the value didn't match our filter or if we didn't find the field keep iterating
		if err != nil || value == nil {
			continue
		}

		// TODO: update any limit-related counters here.

		return tsc.key, timestamp, value
	}
}

// IsEmpty returns whether the tagsetCursor has any more data for the current interval.
func (tsc *tagSetCursor) IsEmptyForInterval() bool {
	for _, c := range tsc.cursors {
		k, _ := c.Peek()
		if k != 0 && k >= tsc.sm.currTmin && k <= tsc.sm.currTmax {
			return false
		}
	}
	return true
}

type seriesCursor struct {
	cursor      *shardCursor // BoltDB cursor for a series
	filter      influxql.Expr
	keyBuffer   int64  // The current timestamp key for the cursor
	valueBuffer []byte // The current value for the cursor
}

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

// Seek positions the cursor at the key, such that Next() will return
// the key and value at key.
func (mc *seriesCursor) Seek(key int64) {
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
