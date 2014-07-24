package coordinator

import (
	"github.com/influxdb/influxdb/cluster"
	"github.com/influxdb/influxdb/parser"
)

// duration 1h, 1d, 7d
// split duration to n shards
// if n > 1
// hash using either random or series name

// These are things that the Coordinator need (defined in Coordinator, will have to import cluster package)
type ShardAwareObject interface {
	GetShards(querySpec *parser.QuerySpec) []cluster.Shard
	// returns true if results from shards can just be ordered. false if the results are raw points that
	// need to be sent through the query engine
	CanCollateShards(querySpec *parser.QuerySpec) bool
	GetShardById(id uint32) cluster.Shard
	GetShardToWriteToBySeriesAndTime(db, series string, microsecondsEpoch int64) (cluster.Shard, error)
}
