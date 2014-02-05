package coordinator

import (
	"cluster"
)

// duration 1h, 1d, 7d
// split duration to n shards
// if n > 1
// hash using either random or series name

// These are things that the Coordinator need (defined in Coordinator, will have to import cluster package)
type ShardAwareObject interface {
	GetShards(querySpec cluster.QuerySpec) []cluster.Shard
	GetShardById(id uint32) cluster.Shard
	GetShardToWriteToBySeriesAndTime(db, series string, microsecondsEpoch int64) (cluster.Shard, error)
}
