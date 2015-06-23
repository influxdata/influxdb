package tsdb

import (
	"fmt"
	"time"

	"github.com/influxdb/influxdb/influxql"
	"github.com/influxdb/influxdb/meta"
)

type Planner struct {
	metaStore interface {
		RetentionPolicy(database, name string) (rpi *meta.RetentionPolicyInfo, err error)
	}

	localStore interface {
		Measurement(database, name string) *Measurement
		Shard(shardID uint64) *Shard
	}
}

func NewPlanner(m metaStore, l localStore) *Planner {
	return &Planner{
		metaStore:  m,
		localStore: l,
	}
}

func (p *Planner) Plan(stmt *influxql.SelectStatement) (*Executor, error) {
	mappers := make([]ShardProcessor, 0)

	for _, src := range stmt.Sources {
		mm, ok := src.(*influxql.Measurement)
		if !ok {
			return nil, fmt.Errorf("invalid source type: %#v", src)
		}

		// Get the target retention policy.
		rp, err := p.metaStore.RetentionPolicy(mm.Database, mm.RetentionPolicy)
		if err != nil {
			return nil, err
		}

		// Grab time range from statement.
		tmin, tmax := influxql.TimeRange(stmt.Condition)
		if tmax.IsZero() {
			tmax = time.Now()
		}
		if tmin.IsZero() {
			tmin = time.Unix(0, 0)
		}

		// Create a Processor for every shard that must be queried.
		for _, group := range rp.ShardGroups {
			if group.Overlaps(tmin, tmax) {
				for _, sh := range group.Shards {
					// Assume shard is local for now.
					mappers = append(mappers, NewLocalShardProcessor(sh.ID, p.xLocalStore))
				}
			}
		}
	}

	return &Executor{mappers: mappers}, nil
}

type LocalShardProcessor struct {
	shardID     uint64
	xLocalStore interface {
		Measurement(database, name string) *Measurement
		Shard(shardID uint64) *Shard
	}
}

func NewLocalShardProcessor(shID uint64, store xLocalStore) *LocalShardProcessor {
	return &LocalShardProcessor{
		shardID:     shID,
		xLocalStore: store,
	}
}

func (l *LocalShardProcessor) Open(stmt *influxql.SelectStatement) error {
	// Determine group by tag keys.
	_, tags, err := stmt.Dimensions.Normalize()
	if err != nil {
		return err
	}

	m := l.store.Measurement(mm.Database, mm.Name)
	if m == nil {
		return nil, ErrMeasurementNotFound(influxql.QuoteIdent([]string{mm.Database, "", mm.Name}...))
	}

	return nil
}

func (l *LocalShardProcessor) Close() {
	return
}

func (l *LocalShardProcessor) NextInterval() (interface{}, error) {
	return nil, nil
}

type ShardProcessor interface {
	// Open will open the necessary resources to begin the processor.
	Open(stmt *influxql.SelectStatement) error

	// Close will close the mapper.
	Close()

	// NextInterval will get the time ordered next interval of the given interval size from the processor.
	NextInterval() (interface{}, error)
}

type Executor struct {
	mappers []ShardProcessor
}
