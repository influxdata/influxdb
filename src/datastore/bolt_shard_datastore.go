package datastore

import (
	"cluster"
	"configuration"
	"protocol"
)

type BoltShardDatastore struct {
}

func NewListmapShardDatastore(config *configuration.Configuration) (*BoltShardDatastore, error) {
	return &BoltShardDatastore{}, nil
}

func (d *BoltShardDatastore) BufferWrite(request *protocol.Request) {

}

func (d *BoltShardDatastore) Close() {

}

func (d *BoltShardDatastore) DeleteShard(shardId uint32) error {
	return nil
}

func (d *BoltShardDatastore) GetOrCreateShard(id uint32) (cluster.LocalShardDb, error) {
	return nil, nil
}

func (d *BoltShardDatastore) ReturnShard(id uint32) {

}

func (d *BoltShardDatastore) SetWriteBuffer(writeBuffer *cluster.WriteBuffer) {

}

func (d *BoltShardDatastore) Write(request *protocol.Request) error {
	return nil
}
