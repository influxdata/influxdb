package coordinator

import (
	"cluster"
	"parser"
	"protocol"
	"time"
)

// duration 1h, 1d, 7d
// split duration to n shards
// if n > 1
// hash using either random or series name

// These are things that the Coordinator need (defined in Coordinator, will have to import cluster package)
type ShardAwareObject interface {
	GetShards(querySpec cluster.QuerySpec) []Shard
	GetShardById(id uint32) Shard
	GetShardToWriteToBySeriesAndTime(series string, t time.Time) Shard
}

// Shard contains data from [startTime, endTime)
// Ids are unique across the cluster
type Shard interface {
	Id() uint32
	StartTime() time.Time
	EndTime() time.Time
	Write([]*protocol.Series) error
	Query(*parser.Query, chan *protocol.Response) error
}
