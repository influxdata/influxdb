package influxdb

import (
	"fmt"
	"math"
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

	itrs []*shardIterator // shard iterators
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
func (tx *tx) CreateMapReduceJobs(stmt *influxql.SelectStatement, tagKeys []string, interval int64) ([]*influxql.MapReduceJob, error) {
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
	for _, f := range stmt.FieldNames() {
		if m.FieldByName(f) == nil {
			return nil, ErrFieldNotFound
		}
	}

	// Grab time range from statement.
	tmin, tmax := influxql.TimeRange(stmt.Condition)
	if tmin.IsZero() {
		tmin = time.Unix(0, 1)
	}
	if tmax.IsZero() {
		tmax = tx.now
	}

	// Find shard groups within time range.
	var shardGroups []*ShardGroup
	for _, group := range rp.shardGroups {
		if timeBetweenInclusive(group.StartTime, tmin, tmax) || timeBetweenInclusive(group.EndTime, tmin, tmax) {
			shardGroups = append(shardGroups, group)
		}
	}
	if len(shardGroups) == 0 {
		return nil, nil
	}

	tagSets := m.tagSets(stmt, tagKeys)

	// Get a field decoder.
	d := NewFieldCodec(m)

	jobs := make([]*influxql.MapReduceJob, 0, len(tagSets))
	for _, t := range tagSets {
		// make a job for each tagset
		job := &influxql.MapReduceJob{
			MeasurementName: m.Name,
			TagSet:          t,
			TMin:            tmin.UnixNano(),
			TMax:            tmin.UnixNano(),
			Interval:        interval,
		}

		// make a mapper for each shard that must be hit. We may need to hit multiple shards within a shard group
		mappers := make([]influxql.Mapper, 0)

		// we'll need map and reduce funcs for each aggregate
		aggregateCalls := stmt.AggregateCalls()
		job.FuncCount = len(aggregateCalls)

		// reduce funcs
		reduceFuncs := make([]influxql.ReduceFunc, len(aggregateCalls))

		// and we'll need the field each aggregate works on
		fieldIDs := make([]uint8, len(aggregateCalls))

		for i, c := range aggregateCalls {
			// Ensure the argument is a variable reference.
			ref, ok := c.Args[0].(*VarRef)
			if !ok {
				return nil, fmt.Errorf("expected field argument in %s()", c.Name)
			}
			fieldIDs[i] = m.FieldByName(ref.Val).ID

			// now set the reduce function. We set the map function with each mapper later on.
			_, reduceFunc, err := influxql.MapReduceFuncs(c)
			if err != nil {
				return nil, err
			}
			reduceFuncs[i] = reduceFunc
		}

		// create mappers for each shard we need to hit
		for _, sg := range shardGroups {
			if len(sg.Shards) != 1 { // we'll only have more than 1 shard in a group when RF < # servers in cluster
				// TODO: implement distributed queries.
				panic("distributed queries not implemented yet and there are too many shards in this group")
			}

			// set the map functions. We do this inside the loop because they may carry some state between calls, so we need a new one for each mapper
			mapFuncs := make([]influxql.MapFunc, len(aggregateCalls))
			for i, c := range aggregateCalls {
				m, _, _ := influxql.MapReduceFuncs()
				mapFuncs[i] = m
			}

			shard := sg.Shards[0]
			mapper := &LocalMapper{
				seriesIDs: t.SeriesIDs(),
				db:        shard.store,
				job:       job,
				mapFuncs:  mapFuncs,
				fieldIDs:  fieldIDs,
				decoder:   fieldDecoder,
			}

			mappers := append(mappers, mapper)
		}

		job.reducer = &influxql.Reducer{Mappers: mappers, ReduceFuncs: reduceFuncs}

		jobs = append(jobs, job)
	}

	// always return them in sorted order so the results from running the jobs are returned in a deterministic order
	sort.Sort(influxql.MapReduceJobs(jobs))
	return jobs, nil
}

// LocalMapper implements the influxql.Mapper interface for running map tasks over a shard that is local to this server
type LocalMapper struct {
	decoder     fieldDecoder           // decoder for the raw data bytes
	tagSet      *influxql.TagSet       // filters to be applied to each series
	filters     []*influxql.Expr       // filters for each series
	cursors     []*bolt.Cursor         // bolt cursors for each series id
	seriesIDs   []uint16               // seriesIDs to be read from this shard
	db          *bolt.DB               // bolt store for the shard accessed by this mapper
	txn         *bolt.Tx               // read transactions by shard id
	job         *influxql.MapReduceJob // the MRJob this mapper belongs to
	mapFuncs    []influxql.MapFunc     // the map functions
	fieldIDs    []uint8                // the field ID that is associated with each mapFunc
	keyBuffer   [][]byte
	valueBuffer [][]byte
	tmin        int64
	tmax        int64
	fieldID     uint8    // the id of the field currently iterating
	startKeys   [][]byte // the keys at the start of a group by intervals
}

func (l *LocalMapper) Open() error {
	// Open the data store
	txn, err := l.db.Begin(false)
	if err != nil {
		return err
	}
	l.txn = txn

	// create a bolt cursor for each unique series id
	l.cursors = make([]*seriesCursor, len(l.seriesIDs), len(l.seriesIDs))
	l.filters = make()
	for i, id := range l.seriesIDs {
		b := i.txn.Bucket(u32tob(c.id))
		if b == nil {
			continue
		}

		l.cursors[i] = b.Cursor()
	}
}

func (l *LocalMapper) Close() {
	_ = i.txn.Rollback()
	return nil
}

func (l *LocalMapper) Run(emitters []influxql.Emitter) {
	// set up the buffers. These ensure that we return data in time order
	l.keyBuffer = make([]int64, len(l.cursors))
	l.valueBuffer = make([][]byte, len(l.cursors))
	l.tmin = math.MaxInt64

	// seek the bolt cursors and fill the buffers
	for i, c := range l.cursors {
		k, v := c.Seek(u64tob(uint64(l.job.TMin)))
		l.startKeys[i] = k
		if k == nil {
			keyBuffer[i] = 0
			valueBuffer[i] = nil
			continue
		}
		t := int64(btou64(k))
		if t < l.tmin {
			l.tmin = t
		}
		keyBuffer[i] = k
		valueBuffer[i] = v
	}

	// tmin should be the start of the group by interval for the first data point
	if l.job.Interval > 0 {
		l.tmin -= (l.tmin % l.job.Interval)
	}

	// now empty the cursors and yield to the map functions
	for {
		// Set the upper bound of the interval.
		if m.interval > 0 {
			l.tmax = l.tmin + l.job.Interval - 1
		}

		// Execute the map functions. The local mapper acts as the iterator for the map functions
		for i, m := range l.mapFuncs {
			l.fieldID = i
			// only rewind if we have to.
			if i != 0 && len(l.mapFuncs) > 1 {
				l.rewind() // rewind to the beginning of this interval to yield to the map func
			}
			m(l, emitters[i], l.tmin)
		}

		// set the start keys to the end keys of the interval or break out if everything is done
		cursorsEmpty := true
		for i, k := range l.keyBuffer {
			if k != nil {
				cursorsEmpty = false
			}
			l.startKeys[i] = k
		}

		if cursorsEmpty { // close out the emitters and return
			for _, e := range emitters {
				_ = e.Close(nil)
			}
			return
		}

		// Move the interval forward.
		l.tmin += m.interval
	}
}

func (l *LocalMapper) Next() (seriesID uint16, timestamp int64, value interface{}) {
	// find the minimum timestamp
	min := -1
	for i, k := range l.keyBuffer {
		if k != nil {
			t := int64(btou64(k))
			if t != 0 && t < l.tmax {
				min = i
			}
		}
	}

	// return if all the cursors have reached the end
	if min == -1 {
		return 0, 0, nil
	}

	// save the curent key and value
	key = l.keyBuffer[min]
	value = l.valueBuffer[min]
	seriesID = l.seriesIDs[min]

	// advance the cursor
	l.keyBuffer[min], l.valueBuffer[min] = l.cursors[min].Next()

	// decode the value and return
	val := l.decoder.DecodeByID(l.fieldID, v)
	return seriesID, int64(btou64(key)), val
}

// rewind resets the cursors to the beginning of a group by interval
func (l *LocalMapper) rewind() {
	for i, k := range l.startKeys {
		if k != nil {
			kk, v := l.cursors[i].Seek(k)
			l.keyBuffer[i] = int64(btou64(kk))
			l.valueBuffer[i] = v
		}
	}
}

func (l *LocalMapper) CurrentFieldValues() map[uint8]interface{} {
	// TODO: implement this
	panic("not implemented")
}

func (l *LocalMapper) Tags(seriesID uint16) map[string]string {
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
