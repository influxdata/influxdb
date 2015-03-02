package influxdb

import (
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/boltdb/bolt"
	"github.com/influxdb/influxdb/influxql"
)

// tx represents a transaction that spans multiple shard data stores.
// This transaction will open and close all data stores atomically.
type tx struct {
	mu     sync.Mutex
	server *Server
	opened bool
	now    time.Time
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
	// Parse the source segments.
	database, policyName, measurement, err := splitIdent(stmt.Source.(*influxql.Measurement).Name)
	if err != nil {
		return nil, err
	}

	// Find database and retention policy.
	db := tx.server.databases[database]
	if db == nil {
		return nil, ErrDatabaseNotFound
	}
	rp := db.policies[policyName]
	if rp == nil {
		return nil, ErrRetentionPolicyNotFound
	}

	// Find measurement.
	m, err := tx.server.measurement(database, measurement)
	if err != nil {
		return nil, err
	}
	if m == nil {
		return nil, ErrMeasurementNotFound
	}

	// Validate the fields asked for exist
	for _, f := range stmt.DatabaseFields() {
		field := m.FieldByName(f.Name)
		if field == nil {
			return nil, ErrFieldNotFound
		}

		// populat the ID on the select statement so it can be used later in the query
		f.ID = field.ID
	}

	// Grab time range from statement.
	tmin, tmax := influxql.TimeRange(stmt.Condition)
	warn("tx: ", tmin, tmax, tmin.UnixNano())
	if tmax.IsZero() {
		tmax = tx.now
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

	// get the sorted unique tag sets for this query.
	tagSets := m.tagSets(stmt, tagKeys)

	jobs := make([]*influxql.MapReduceJob, 0, len(tagSets))
	for _, t := range tagSets {
		// make a job for each tagset
		job := &influxql.MapReduceJob{
			MeasurementName: m.Name,
			TagSet:          t,
			TMin:            tmin.UnixNano(),
			TMax:            tmax.UnixNano(),
		}

		// make a mapper for each shard that must be hit. We may need to hit multiple shards within a shard group
		mappers := make([]influxql.Mapper, 0)

		// create mappers for each shard we need to hit
		for _, sg := range shardGroups {
			if len(sg.Shards) != 1 { // we'll only have more than 1 shard in a group when RF < # servers in cluster
				// TODO: implement distributed queries.
				panic("distributed queries not implemented yet and there are too many shards in this group")
			}

			shard := sg.Shards[0]
			mapper := &LocalMapper{
				seriesIDs: t.SeriesIDs,
				db:        shard.store,
				job:       job,
				decoder:   NewFieldCodec(m),
				filters:   t.Filters,
			}

			mappers = append(mappers, mapper)
		}

		job.Mappers = mappers

		jobs = append(jobs, job)
	}

	// always return them in sorted order so the results from running the jobs are returned in a deterministic order
	sort.Sort(influxql.MapReduceJobs(jobs))
	return jobs, nil
}

// LocalMapper implements the influxql.Mapper interface for running map tasks over a shard that is local to this server
type LocalMapper struct {
	cursorsEmpty bool                   // boolean that lets us know if the cursors are empty
	decoder      fieldDecoder           // decoder for the raw data bytes
	tagSet       *influxql.TagSet       // filters to be applied to each series
	filters      []influxql.Expr        // filters for each series
	cursors      []*bolt.Cursor         // bolt cursors for each series id
	seriesIDs    []uint32               // seriesIDs to be read from this shard
	db           *bolt.DB               // bolt store for the shard accessed by this mapper
	txn          *bolt.Tx               // read transactions by shard id
	job          *influxql.MapReduceJob // the MRJob this mapper belongs to
	mapFunc      influxql.MapFunc       // the map func
	fieldID      uint8                  // the field ID associated with the mapFunc
	keyBuffer    []int64                // the current timestamp key for each cursor
	valueBuffer  [][]byte               // the current value for each cursor
	tmin         int64                  // the min of the current group by interval being iterated over
	tmax         int64                  // the max of the current group by interval being iterated over
}

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
		b := l.txn.Bucket(u32tob(id))
		if b == nil {
			continue
		}

		l.cursors[i] = b.Cursor()
	}

	return nil
}

func (l *LocalMapper) Close() {
	_ = l.txn.Rollback()
}

// Begin will set up the mapper to run the map function for a given aggregate call starting at the passed in time
func (l *LocalMapper) Begin(c *influxql.Call, startingTime int64) error {
	// set up the buffers. These ensure that we return data in time order
	mapFunc, err := influxql.InitializeMapFunc(c)
	if err != nil {
		return err
	}
	l.mapFunc = mapFunc
	l.keyBuffer = make([]int64, len(l.cursors))
	l.valueBuffer = make([][]byte, len(l.cursors))
	l.tmin = startingTime

	// seek the bolt cursors and fill the buffers
	for i, c := range l.cursors {
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
func (l *LocalMapper) NextInterval(interval int64) (interface{}, error) {
	warn("NextInterval ", l.tmin, l.tmax, interval)
	if l.cursorsEmpty || l.tmin > l.job.TMax {
		return nil, nil
	}

	// Set the upper bound of the interval.
	if interval > 0 {
		l.tmax = l.tmin + interval - 1
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

	// Move the interval forward.
	l.tmin += interval

	return val, nil
}

func (l *LocalMapper) Next() (seriesID uint32, timestamp int64, value interface{}) {
	warn("Next")
	for {
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

		// save the curent key and value
		timestamp = l.keyBuffer[min]
		value, err := l.decoder.DecodeByID(l.fieldID, l.valueBuffer[min])

		// advance the cursor
		nextKey, nextVal := l.cursors[min].Next()
		if nextKey == nil {
			l.keyBuffer[min] = 0
		} else {
			l.keyBuffer[min] = int64(btou64(nextKey))
		}
		l.valueBuffer[min] = nextVal

		// if we didn't find the field keep iterating
		if err != nil {
			continue
		}
		seriesID = l.seriesIDs[min]

		if value == nil {
			return 0, 0, nil
		}

		return seriesID, timestamp, value
	}
}

func (l *LocalMapper) CurrentFieldValues() map[uint8]interface{} {
	// TODO: implement this
	panic("not implemented")
}

func (l *LocalMapper) Tags(seriesID uint32) map[string]string {
	// TODO: implement this
	panic("not implemented")
}

// splitIdent splits an identifier into it's database, policy, and measurement parts.
func splitIdent(s string) (db, rp, m string, err error) {
	a, err := influxql.SplitIdent(s)
	if err != nil {
		return "", "", "", err
	} else if len(a) != 3 {
		return "", "", "", fmt.Errorf("invalid ident, expected 3 segments: %q", s)
	}
	return a[0], a[1], a[2], nil
}

type fieldDecoder interface {
	DecodeByID(fieldID uint8, b []byte) (interface{}, error)
}
