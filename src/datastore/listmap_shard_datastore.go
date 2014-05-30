package datastore

import (
	"cluster"
	"configuration"
	"protocol"
)

type ListmapShardDatastore struct {
}

func NewListmapShardDatastore(config *configuration.Configuration) (*ListmapShardDatastore, error) {
	return &ListmapShardDatastore{}, nil
}

func (d *ListmapShardDatastore) BufferWrite(request *protocol.Request) {

}

func (d *ListmapShardDatastore) Close() {

}

func (d *ListmapShardDatastore) DeleteShard(shardId uint32) error {
	return nil
}

func (d *ListmapShardDatastore) GetOrCreateShard(id uint32) (cluster.LocalShardDb, error) {
	return nil, nil
}

func (d *ListmapShardDatastore) ReturnShard(id uint32) {

}

func (d *ListmapShardDatastore) SetWriteBuffer(writeBuffer *cluster.WriteBuffer) {

}

func (d *ListmapShardDatastore) Write(request *protocol.Request) error {
	return nil
}
