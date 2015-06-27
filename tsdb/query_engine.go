package tsdb

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/boltdb/bolt"
	"github.com/influxdb/influxdb/influxql"
	"github.com/influxdb/influxdb/meta"
)

type Mapper interface {
	// Open will open the necessary resources to being the map job. Could be connections to remote servers or
	// hitting the local store
	Open() error

	// Close will close the mapper
	Close()

	// Begin will set up the Mapper to return series data for the given query.
	Begin(stmt *influxql.SelectStatement, chunkSize int) error

	// NextChunk returns the next chunk of data within the interval, for a specific tag set.
	// interval is a monotonically increasing number based on the group by time and the shard
	// times. It lets the caller know when mappers are processing the same interval
	NextChunk() (tagSet string, result interface{}, interval int, err error)
}

type Planner struct {
	MetaStore interface {
		ShardGroupsByTimeRange(database, policy string, min, max time.Time) (a []meta.ShardGroupInfo, err error)
		NodeID() uint64
	}

	Cluster interface {
		NewMapper(shardID uint64) (Mapper, error)
	}

	// Local store for local shards. Would be elegant if all shards came from Cluster. REVISIT THIS!
	// The TSDBSTORE interface in the cluster service could be expanded to "ShardMapper(shard ID)"
	// this may not be possible due to circular imports. This file couldn't import Cluster, since
	// imports this package.
	store *Store

	Logger *log.Logger
}

func NewPlanner() *Planner {
	return &Planner{
		Logger: log.New(os.Stderr, "[planner] ", log.LstdFlags),
	}
}

// Plan creates an execution plan for the given SelectStatement and returns an Executor.
func (p *Planner) Plan(stmt *influxql.SelectStatement, chunkSize int) (*Executor, error) {
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

	// Build the Mappers, one per shard. If the shard is local to this node, always use
	// that one, versus asking the cluster.
	mappers := []Mapper{}
	for _, sh := range shards {
		if sh.OwnedBy(p.MetaStore.NodeID()) {
			shard := p.store.Shard(sh.ID)
			if shard == nil {
				// If the store returns nil, no data has actually been written to the shard.
				// In this case, since there is no data, don't make a mapper.
				continue
			}
			mappers = append(mappers, NewShardMapper(shard))
		} else {
			mapper, err := p.Cluster.NewMapper(sh.ID)
			if err != nil {
				return nil, err
			}
			if mapper == nil {
				// No error, but there shard doesn't actually exist anywhere on the cluster.
				// This means no data has been written to it, so forget about the mapper.
				continue
			}
			mappers = append(mappers, mapper)
		}

	}

	return NewExecutor(mappers), nil
}

type Executor struct {
	mappers []Mapper
}

func NewExecutor(mappers []Mapper) *Executor {
	return &Executor{
		mappers: mappers,
	}
}

// Execute begins execution of the query and returns a channel to receive rows.
func (e *Executor) Execute() <-chan *influxql.Row {
	// Create output channel and stream data in a separate goroutine.
	out := make(chan *influxql.Row, 0)

	return out
}

type ShardMapper struct {
	shard *Shard

	tx      *bolt.Tx // Read transaction for this shard.
	cursors []*mapperCursor

	queryTMin time.Time     // the min specified by the query
	queryTMax time.Time     // the max specified by the query
	currTmin  time.Time     // the min of the current GROUP BY interval being iterated over
	currTmax  time.Time     // the max of the current GROUP BY interval being iterated over
	interval  time.Duration // GROUP BY interval.
}

func NewShardMapper(shard *Shard) *ShardMapper {
	return &ShardMapper{
		shard:   shard,
		cursors: make([]*mapperCursor, 0),
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
	for _, src := range stmt.Sources {
		mm := src.(*influxql.Measurement)

		m := sm.shard.index.Measurement(mm.Name)
		if m == nil {
			return ErrMeasurementNotFound(influxql.QuoteIdent([]string{mm.Database, "", mm.Name}...))
		}

		// Set all time-related paremeters on the mapper.
		sm.queryTMin, sm.queryTMax = influxql.TimeRange(stmt.Condition)
		if sm.queryTMin.IsZero() {
			sm.queryTMin = time.Unix(0, 0)
		}
		if sm.queryTMax.IsZero() {
			sm.queryTMax = time.Now()
		}
		sm.currTmin = sm.queryTMin
		sm.currTmax = sm.queryTMax
		if sm.interval, err = stmt.GroupByInterval(); err != nil {
			return err
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
			for _, key := range t.SeriesKeys {
				c := sm.createCursorForSeries(key)
				if c == nil {
					// No data exists for this key.
					continue
				}
				cm := &mapperCursor{
					Cursor:  c,
					Filters: t.Filters,
					Decoder: sm.shard.FieldCodec(m.Name),
				}
				cm.Seek(u64tob(uint64(sm.queryTMin.UnixNano())))
				sm.cursors = append(sm.cursors, cm)
			}
		}
	}

	return nil
}

func (sm *ShardMapper) NextChunk() (tagSet string, result interface{}, interval int, err error) {
	if sm.IsEmpty(uint64(sm.currTmax.UnixNano())) || sm.currTmin.After(sm.queryTMax) {
		return "", nil, 1, nil
	}
	return "", nil, 1, nil
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

// IsEmpty returns true if either all cursors are nil or all cursors are past the give time.
func (sm *ShardMapper) IsEmpty(tmax uint64) bool {
	for _, c := range sm.cursors {
		k, _ := c.Peek()
		if k != nil && uint64(btou64(k)) <= tmax {
			return false
		}
	}

	return true
}

type mapperCursor struct {
	Cursor  *shardCursor    // BoltDB cursor for a series
	Filters []influxql.Expr // Filters for the series
	Decoder *FieldCodec     // Decoder for the raw data bytes

	keyBuffer   []byte // The current timestamp key for the cursor
	valueBuffer []byte // The current value for the cursor
}

// Peek returns the next key and value, without changing what will be
// be returned by a call to Next()
func (mc *mapperCursor) Peek() (key, value []byte) {
	if mc.keyBuffer != nil {
		mc.keyBuffer, mc.valueBuffer = mc.Cursor.Next()
	}

	key, value = mc.keyBuffer, mc.valueBuffer
	return
}

// Seek positions the cursor at the key, such that Next will return
// the key and value at key.
func (mc *mapperCursor) Seek(key []byte) {
	mc.keyBuffer, mc.valueBuffer = mc.Cursor.Seek(key)
}

// Next returns the next key and value from the cursor.
func (mc *mapperCursor) Next() (key, value []byte) {
	if mc.keyBuffer != nil {
		key, value = mc.keyBuffer, mc.valueBuffer
		mc.keyBuffer, mc.valueBuffer = nil, nil
	} else {
		key, value = mc.Cursor.Next()
	}
	return
}
