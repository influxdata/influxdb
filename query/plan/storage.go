package plan

import "time"

type Storage interface {
	ShardMapping() ShardMap
}

// ShardMap is a mapping of database names to list of shards for that database.
type ShardMap map[string][]Shard

type Shard struct {
	Node  string
	Range TimeRange
}

type TimeRange struct {
	Start time.Time
	Stop  time.Time
}
