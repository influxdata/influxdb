package influxdb

import (
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/boltdb/bolt"
	"github.com/influxdb/influxdb/influxql"
)

// tx represents a transaction that spans multiple shard data stores.
// This transaction will open and close all data stores atomically.
type tx struct {
	server *Server
	now    time.Time

	// used by DecodeFields and FieldIDs. Only used in a raw query, which won't let you select from more than one measurement
	measurement *Measurement
	decoder     fieldDecoder
}

// newTx return a new initialized Tx.
func newTx(server *Server) *tx {
	return &tx{
		server: server,
		now:    time.Now(),
	}
}

// SetNow sets the current time for the transaction.
func (tx *tx) SetNow(now time.Time) { tx.now = now }

// CreateMappers will create a set of mappers that need to be run to execute the map phase of a MapReduceJob.
func (tx *tx) CreateMapReduceJobs(stmt *influxql.SelectStatement, tagKeys []string) ([]*influxql.MapReduceJob, error) {
	jobs := []*influxql.MapReduceJob{}
	for _, src := range stmt.Sources {
		mm, ok := src.(*influxql.Measurement)
		if !ok {
			return nil, fmt.Errorf("invalid source type: %#v", src)
		}

		// Find database and retention policy.
		db := tx.server.databases[mm.Database]
		if db == nil {
			return nil, ErrDatabaseNotFound(mm.Database)
		}
		rp := db.policies[mm.RetentionPolicy]
		if rp == nil {
			return nil, ErrRetentionPolicyNotFound
		}

		// Find measurement.
		m, err := tx.server.measurement(mm.Database, mm.Name)
		if err != nil {
			return nil, err
		}
		if m == nil {
			return nil, ErrMeasurementNotFound(influxql.QuoteIdent([]string{mm.Database, "", mm.Name}...))
		}

		tx.measurement = m
		tx.decoder = NewFieldCodec(m)

		// Validate the fields and tags asked for exist and keep track of which are in the select vs the where
		var selectFields []*Field
		var whereFields []*Field
		var selectTags []string

		for _, n := range stmt.NamesInSelect() {
			f := m.FieldByName(n)
			if f != nil {
				selectFields = append(selectFields, f)
				continue
			}
			if !m.HasTagKey(n) {
				return nil, fmt.Errorf("unknown field or tag name in select clause: %s", n)
			}
			selectTags = append(selectTags, n)
		}
		for _, n := range stmt.NamesInWhere() {
			if n == "time" {
				continue
			}
			f := m.FieldByName(n)
			if f != nil {
				whereFields = append(whereFields, f)
				continue
			}
			if !m.HasTagKey(n) {
				return nil, fmt.Errorf("unknown field or tag name in where clause: %s", n)
			}
		}

		// If a numerical aggregate is requested, ensure it is only performed on numeric data or on a
		// nested aggregate on numeric data.
		for _, a := range stmt.FunctionCalls() {
			// Check for fields like `derivative(mean(value), 1d)`
			var nested *influxql.Call = a
			if fn, ok := nested.Args[0].(*influxql.Call); ok {
				nested = fn
			}

			lit, ok := nested.Args[0].(*influxql.VarRef)
			if !ok {
				return nil, fmt.Errorf("aggregate call didn't contain a field %s", a.String())
			}
			if influxql.IsNumeric(nested) {
				f := m.FieldByName(lit.Val)
				if f.Type != influxql.Float && f.Type != influxql.Integer {
					return nil, fmt.Errorf("aggregate '%s' requires numerical field values. Field '%s' is of type %s",
						a.Name, f.Name, f.Type)
				}
			}
		}

		// Grab time range from statement.
		tmin, tmax := influxql.TimeRange(stmt.Condition)
		if tmax.IsZero() {
			tmax = tx.now
		}
		if tmin.IsZero() {
			tmin = time.Unix(0, 0)
		}

		// Find shard groups within time range.
		var shardGroups []*ShardGroup
		for _, group := range rp.shardGroups {
			if group.Contains(tmin, tmax) {
				shardGroups = append(shardGroups, group)
			}
		}
		if len(shardGroups) == 0 {
			return nil, nil
		}

		// get the group by interval, if there is one
		var interval int64
		if d, err := stmt.GroupByInterval(); err != nil {
			return nil, err
		} else {
			interval = d.Nanoseconds()
		}

		// get the sorted unique tag sets for this query.
		tagSets, err := m.tagSets(stmt, tagKeys)
		if err != nil {
			return nil, err
		}

		//jobs := make([]*influxql.MapReduceJob, 0, len(tagSets))
		for _, t := range tagSets {
			// make a job for each tagset
			job := &influxql.MapReduceJob{
				MeasurementName: m.Name,
				TagSet:          t,
				TMin:            tmin.UnixNano(),
				TMax:            tmax.UnixNano(),
			}

			// make a mapper for each shard that must be hit. We may need to hit multiple shards within a shard group
			var mappers []influxql.Mapper

			// create mappers for each shard we need to hit
			for _, sg := range shardGroups {

				shards := map[*Shard][]uint64{}
				for _, sid := range t.SeriesIDs {
					shard := sg.ShardBySeriesID(sid)
					shards[shard] = append(shards[shard], sid)
				}

				for shard, sids := range shards {
					var mapper influxql.Mapper

					// create either a remote or local mapper for this shard
					if shard.store == nil {
						nodes := tx.server.DataNodesByID(shard.DataNodeIDs)
						if len(nodes) == 0 {
							return nil, ErrShardNotFound
						}

						balancer := NewDataNodeBalancer(nodes)

						mapper = &RemoteMapper{
							dataNodes:       balancer,
							Database:        mm.Database,
							MeasurementName: m.Name,
							TMin:            tmin.UnixNano(),
							TMax:            tmax.UnixNano(),
							SeriesIDs:       sids,
							ShardID:         shard.ID,
							WhereFields:     whereFields,
							SelectFields:    selectFields,
							SelectTags:      selectTags,
							Limit:           stmt.Limit,
							Offset:          stmt.Offset,
							Interval:        interval,
						}
						mapper.(*RemoteMapper).SetFilters(t.Filters)
					} else {
						mapper = &LocalMapper{
							seriesIDs:    sids,
							db:           shard.store,
							job:          job,
							decoder:      NewFieldCodec(m),
							filters:      t.Filters,
							whereFields:  whereFields,
							selectFields: selectFields,
							selectTags:   selectTags,
							tmin:         tmin.UnixNano(),
							tmax:         tmax.UnixNano(),
							interval:     interval,
							// multiple mappers may need to be merged together to get the results
							// for a raw query. So each mapper will have to read at least the
							// limit plus the offset in data points to ensure we've hit our mark
							limit: uint64(stmt.Limit) + uint64(stmt.Offset),
						}
					}

					mappers = append(mappers, mapper)
				}
			}

			job.Mappers = mappers

			jobs = append(jobs, job)
		}
	}

	// always return them in sorted order so the results from running the jobs are returned in a deterministic order
	sort.Sort(influxql.MapReduceJobs(jobs))
	return jobs, nil
}

// DecodeValues is for use in a raw data query
func (tx *tx) DecodeValues(fieldIDs []uint8, timestamp int64, data []byte) []interface{} {
	vals := make([]interface{}, len(fieldIDs)+1)
	vals[0] = timestamp
	for i, id := range fieldIDs {
		v, _ := tx.decoder.DecodeByID(id, data)
		vals[i+1] = v
	}
	return vals
}

// FieldIDs will take an array of fields and return the id associated with each
func (tx *tx) FieldIDs(fields []*influxql.Field) ([]uint8, error) {
	names := tx.fieldNames(fields)
	ids := make([]uint8, len(names))

	for i, n := range names {
		field := tx.measurement.FieldByName(n)
		if field == nil {
			return nil, ErrFieldNotFound
		}
		ids[i] = field.ID
	}

	return ids, nil
}

// fieldNames returns the referenced database field names from the slice of fields
func (tx *tx) fieldNames(fields []*influxql.Field) []string {
	var a []string
	for _, f := range fields {
		if v, ok := f.Expr.(*influxql.VarRef); ok { // this is a raw query so we handle it differently
			a = append(a, v.Val)
		}
	}
	return a
}

// LocalMapper implements the influxql.Mapper interface for running map tasks over a shard that is local to this server
type LocalMapper struct {
	cursorsEmpty     bool                   // boolean that lets us know if the cursors are empty
	decoder          fieldDecoder           // decoder for the raw data bytes
	filters          []influxql.Expr        // filters for each series
	cursors          []*bolt.Cursor         // bolt cursors for each series id
	seriesIDs        []uint64               // seriesIDs to be read from this shard
	db               *bolt.DB               // bolt store for the shard accessed by this mapper
	txn              *bolt.Tx               // read transactions by shard id
	job              *influxql.MapReduceJob // the MRJob this mapper belongs to
	mapFunc          influxql.MapFunc       // the map func
	fieldID          uint8                  // the field ID associated with the mapFunc curently being run
	fieldName        string                 // the field name associated with the mapFunc currently being run
	keyBuffer        []int64                // the current timestamp key for each cursor
	valueBuffer      [][]byte               // the current value for each cursor
	tmin             int64                  // the min of the current group by interval being iterated over
	tmax             int64                  // the max of the current group by interval being iterated over
	additionalNames  []string               // additional field or tag names that might be requested from the map function
	whereFields      []*Field               // field names that occur in the where clause
	selectFields     []*Field               // field names that occur in the select clause
	selectTags       []string               // tag keys that occur in the select clause
	isRaw            bool                   // if the query is a non-aggregate query
	interval         int64                  // the group by interval of the query, if any
	limit            uint64                 // used for raw queries for LIMIT
	perIntervalLimit int                    // used for raw queries to determine how far into a chunk we are
	chunkSize        int                    // used for raw queries to determine how much data to read before flushing to client
}

// Open opens the LocalMapper.
func (l *LocalMapper) Open() error {
	// Open the data store
	txn, err := l.db.Begin(false)
	if err != nil {
		return err
	}
	l.txn = txn

	// create a bolt cursor for each unique series id
	l.cursors = make([]*bolt.Cursor, len(l.seriesIDs))

	for i, id := range l.seriesIDs {
		b := l.txn.Bucket(u64tob(id))
		if b == nil {
			continue
		}

		l.cursors[i] = b.Cursor()
	}

	return nil
}

// Close closes the LocalMapper.
func (l *LocalMapper) Close() {
	_ = l.txn.Rollback()
}

// Begin will set up the mapper to run the map function for a given aggregate call starting at the passed in time
func (l *LocalMapper) Begin(c *influxql.Call, startingTime int64, chunkSize int) error {
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

	// determine if this is a raw data query with a single field, multiple fields, or an aggregate
	var fieldName string
	if c == nil { // its a raw data query
		l.isRaw = true
		if len(l.selectFields) == 1 {
			fieldName = l.selectFields[0].Name
		}

		// if they haven't set a limit, just set it to the max int size
		if l.limit == 0 {
			l.limit = math.MaxUint64
		}
	} else {
		// Check for calls like `derivative(mean(value), 1d)`
		var nested *influxql.Call = c
		if fn, ok := c.Args[0].(*influxql.Call); ok {
			nested = fn
		}

		lit, ok := nested.Args[0].(*influxql.VarRef)
		if !ok {
			return fmt.Errorf("aggregate call didn't contain a field %s", c.String())
		}
		fieldName = lit.Val
	}

	// set up the field info if a specific field was set for this mapper
	if fieldName != "" {
		f := l.decoder.FieldByName(fieldName)
		if f == nil {
			return fmt.Errorf("%s isn't a field on measurement %s", fieldName, l.job.MeasurementName)
		}
		l.fieldID = f.ID
		l.fieldName = f.Name
	}

	// seek the bolt cursors and fill the buffers
	for i, c := range l.cursors {
		// this series may have never been written in this shard group (time range) so the cursor would be nil
		if c == nil {
			l.keyBuffer[i] = 0
			l.valueBuffer[i] = nil
			continue
		}
		k, v := c.Seek(u64tob(uint64(l.job.TMin)))
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

// NextInterval will get the time ordered next interval of the given interval size from the mapper. This is a
// forward only operation from the start time passed into Begin. Will return nil when there is no more data to be read.
// If this is a raw query, interval should be the max time to hit in the query
func (l *LocalMapper) NextInterval() (interface{}, error) {
	if l.cursorsEmpty || l.tmin > l.job.TMax {
		return nil, nil
	}

	// after we call to the mapper, this will be the tmin for the next interval.
	nextMin := l.tmin + l.interval

	// Set the upper bound of the interval.
	if l.isRaw {
		l.perIntervalLimit = l.chunkSize
	} else if l.interval > 0 {
		// Set tmax to ensure that the interval lands on the boundary of the interval
		if l.tmin%l.interval != 0 {
			// the first interval in a query with a group by may be smaller than the others. This happens when they have a
			// where time > clause that is in the middle of the bucket that the group by time creates. That will be the
			// case on the first interval when the tmin % the interval isn't equal to zero
			nextMin = l.tmin/l.interval*l.interval + l.interval
		}
		l.tmax = nextMin - 1
	}

	// Execute the map function. This local mapper acts as the iterator
	val := l.mapFunc(l)

	// see if all the cursors are empty
	l.cursorsEmpty = true
	for _, k := range l.keyBuffer {
		if k != 0 {
			l.cursorsEmpty = false
			break
		}
	}

	// Move the interval forward if it's not a raw query. For raw queries we use the limit to advance intervals.
	if !l.isRaw {
		l.tmin = nextMin
	}

	return val, nil
}

// Next returns the next matching timestamped value for the LocalMapper.
func (l *LocalMapper) Next() (seriesID uint64, timestamp int64, value interface{}) {
	for {
		// if it's a raw query and we've hit the limit of the number of points to read in
		// for either this chunk or for the absolute query, bail
		if l.isRaw && (l.limit == 0 || l.perIntervalLimit == 0) {
			return uint64(0), int64(0), nil
		}

		// find the minimum timestamp
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
			return 0, 0, nil
		}

		// set the current timestamp and seriesID
		timestamp = l.keyBuffer[min]
		seriesID = l.seriesIDs[min]

		// decode either the value, or values we need. Also filter if necessary
		var value interface{}
		var err error
		if l.isRaw && len(l.selectFields) > 1 {
			if fieldsWithNames, err := l.decoder.DecodeFieldsWithNames(l.valueBuffer[min]); err == nil {
				value = fieldsWithNames

				// if there's a where clause, make sure we don't need to filter this value
				if l.filters[min] != nil {
					if !matchesWhere(l.filters[min], fieldsWithNames) {
						value = nil
					}
				}
			}
		} else {
			value, err = l.decoder.DecodeByID(l.fieldID, l.valueBuffer[min])

			// if there's a where clase, see if we need to filter
			if l.filters[min] != nil {
				// see if the where is only on this field or on one or more other fields. if the latter, we'll have to decode everything
				if len(l.whereFields) == 1 && l.whereFields[0].ID == l.fieldID {
					if !matchesWhere(l.filters[min], map[string]interface{}{l.fieldName: value}) {
						value = nil
					}
				} else { // decode everything
					fieldsWithNames, err := l.decoder.DecodeFieldsWithNames(l.valueBuffer[min])
					if err != nil || !matchesWhere(l.filters[min], fieldsWithNames) {
						value = nil
					}
				}
			}
		}

		// advance the cursor
		nextKey, nextVal := l.cursors[min].Next()
		if nextKey == nil {
			l.keyBuffer[min] = 0
		} else {
			l.keyBuffer[min] = int64(btou64(nextKey))
		}
		l.valueBuffer[min] = nextVal

		// if the value didn't match our filter or if we didn't find the field keep iterating
		if err != nil || value == nil {
			continue
		}

		// if it's a raw query, we always limit the amount we read in
		if l.isRaw {
			l.limit--
			l.perIntervalLimit--
		}

		return seriesID, timestamp, value
	}
}

// IsEmpty returns true if either all cursors are nil or all cursors are past the passed in max time
func (l *LocalMapper) IsEmpty(tmax int64) bool {
	if l.cursorsEmpty || l.limit == 0 {
		return true
	}

	// look at the next time for each cursor
	for _, t := range l.keyBuffer {
		// if the time is less than the max, we haven't emptied this mapper yet
		if t != 0 && t <= tmax {
			return false
		}
	}

	return true
}

// matchesFilter returns true if the value matches the where clause
func matchesWhere(f influxql.Expr, fields map[string]interface{}) bool {
	if ok, _ := influxql.Eval(f, fields).(bool); !ok {
		return false
	}
	return true
}

type fieldDecoder interface {
	DecodeByID(fieldID uint8, b []byte) (interface{}, error)
	FieldByName(name string) *Field
	DecodeFieldsWithNames(b []byte) (map[string]interface{}, error)
}
