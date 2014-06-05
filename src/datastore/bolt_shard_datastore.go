package datastore

import (
	"cluster"
	"configuration"
	"protocol"

	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type BoltShardDatastore struct {
	baseDir     string
	config      *configuration.Configuration
	writeBuffer *cluster.WriteBuffer
	shards      map[uint32]*BoltShard
	shardMutex  *sync.RWMutex
}

func NewBoltShardDatastore(config *configuration.Configuration) (*BoltShardDatastore, error) {
	baseDir := filepath.Join(config.DataDir, "shards")
	err := os.MkdirAll(baseDir, 0744)
	if err != nil {
		return nil, err
	}

	return &BoltShardDatastore{
		baseDir:    baseDir,
		config:     config,
		shards:     make(map[uint32]*BoltShard),
		shardMutex: &sync.RWMutex{},
	}, nil
}

func (d *BoltShardDatastore) BufferWrite(request *protocol.Request) {
	d.writeBuffer.Write(request)
}

func (d *BoltShardDatastore) Close() {
	for _, shard := range d.shards {
		shard.close()
	}
}

func (d *BoltShardDatastore) DeleteShard(shardId uint32) error {
	shardDir := filepath.Join(d.baseDir, fmt.Sprint(shardId))
	err := os.RemoveAll(shardDir)
	if err != nil {
		return err
	}

	delete(d.shards, shardId)
	return nil
}

func (d *BoltShardDatastore) GetOrCreateShard(id uint32) (cluster.LocalShardDb, error) {
	var (
		shard   *BoltShard
		present bool
	)
	if shard, present = d.shards[id]; !present {
		shardPath := filepath.Join(d.baseDir, fmt.Sprint(id))
		// create a shard
		if err := os.MkdirAll(shardPath, 0744); err != nil {
			return nil, err
		}

		shard = NewBoltShard(shardPath)
		d.shards[id] = shard
	}
	return shard, nil
}

func (d *BoltShardDatastore) ReturnShard(id uint32) {
	d.shards[id].close()
	delete(d.shards, id)
}

func (d *BoltShardDatastore) SetWriteBuffer(writeBuffer *cluster.WriteBuffer) {
	d.writeBuffer = writeBuffer
}

func (d *BoltShardDatastore) Write(request *protocol.Request) error {
	shardDb, err := d.GetOrCreateShard(*request.ShardId)
	if err != nil {
		return err
	}
	return shardDb.Write(*request.Database, request.MultiSeries)
}
