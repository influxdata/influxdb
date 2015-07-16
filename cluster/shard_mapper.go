package cluster

import (
	"github.com/influxdb/influxdb/tsdb"
)

// ShardMapper is responsible for providing mappers for requested shards. It is
// responsible for creating those mappers from the local store, or reaching
// out to another node on the cluster.
type ShardMapper struct {
	MetaStore interface {
		NodeID() uint64
	}

	TSDBStore interface {
		CreateMapper(shardID uint64, query string, chunkSize int) (tsdb.Mapper, error)
	}

	RemoteMapper interface {
		CreateMapper(nodeID uint64, shardID, query string, chunkSize int)
	}
}

func NewShardMapper() *ShardMapper {
	return &ShardMapper{}
}

func (r *ShardMapper) CreateMapper(shardID uint64, stmt string, chunkSize int) (tsdb.Mapper, error) {
	// Shard is local for now.
	m, err := r.TSDBStore.CreateMapper(shardID, stmt, chunkSize)
	if err != nil {
		return nil, err
	}
	return m, nil
}
