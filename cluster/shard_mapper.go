package cluster

import (
	"github.com/influxdb/influxdb/meta"
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
		CreateMapper(nodeID, shardID uint64, query string, chunkSize int) (tsdb.Mapper, error)
	}
}

func NewShardMapper() *ShardMapper {
	return &ShardMapper{}
}

func (r *ShardMapper) CreateMapper(sh meta.ShardInfo, stmt string, chunkSize int) (tsdb.Mapper, error) {
	var err error
	var m tsdb.Mapper
	if sh.OwnedBy(r.MetaStore.NodeID()) {
		m, err = r.TSDBStore.CreateMapper(sh.ID, stmt, chunkSize)
		if err != nil {
			return nil, err
		}
	} else {
		// Pick first available node for now. This will be replaced by balancing.
		m, err = r.RemoteMapper.CreateMapper(sh.OwnerIDs[0], sh.ID, stmt, chunkSize)
		if err != nil {
			return nil, err
		}
	}

	return m, nil
}
