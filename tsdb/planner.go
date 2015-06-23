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
		Shard(shardID uint64) *Shard
	}
}

func NewPlanner(m metaStore) *Planner {
	return &Planner{metaStore: m}
}

func (p *Planner) Plan(stmt *influxql.SelectStatement) (*Executor, error) {
	mappers := make([]ShardMapper, 0)

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

		// Create a Mapper for every shard that must be queried.
		for _, group := range rp.ShardGroups {
			if group.Overlaps(tmin, tmax) {
				for _, sh := range group.Shards {
					// Assume shard is local for now.
					mappers = append(mappers, NewLocalShardMapper(sh.ID))
				}
			}
		}
	}

	return &Executor{mappers: mappers}, nil
}

type LocalShardMapper struct {
}

func NewLocalShardMapper(shID uint64) *LocalShardMapper {
	return &LocalShardMapper{}
}

func (l *LocalShardMapper) Open(query *influxql.Statement) error {
	return nil
}

func (l *LocalShardMapper) Close() {
	return
}

func (l *LocalShardMapper) NextInterval() (interface{}, error) {
	return nil, nil
}

type ShardMapper interface {
	// Open will open the necessary resources to begin the mapper.
	Open(query *influxql.Statement) error

	// Close will close the mapper.
	Close()

	// NextInterval will get the time ordered next interval of the given interval size from the mapper. This is a
	// forward only operation from the start time passed into Begin. Will return nil when there is no more data to be read.
	// Interval periods can be different based on time boundaries (months, daylight savings, etc) of the query.
	NextInterval() (interface{}, error)
}

type Executor struct {
	mappers []ShardMapper
}
