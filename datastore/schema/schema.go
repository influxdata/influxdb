package schema

import "github.com/influxdb/influxdb/cluster"

// Interface to all shard schemas
type Schema interface {
	cluster.LocalShardDb
	Close()
}
