package engine

import (
	p "github.com/influxdb/influxdb/protocol"
)

type QueryProcessor interface {
	// This method returns true if the query should continue. If the query should be stopped,
	// like maybe the limit was hit, it should return false
	YieldPoint(seriesName *string, columnNames []string, point *p.Point) bool
	YieldSeries(seriesIncoming *p.Series) bool
	Close()

	// Set by the shard, so EXPLAIN query can know query against which shard is being measured
	SetShardInfo(shardId int, shardLocal bool)

	// Let QueryProcessor identify itself. What if it is a spy and we can't check that?
	GetName() string
}
