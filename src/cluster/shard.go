package cluster

import (
	"parser"
	"protocol"
	"time"
)

// A shard imements an interface for writing and querying data.
// It can be copied to multiple servers.

type Shard struct {
	id        uint32
	startTime time.Time
	endTime   time.Time
}

type RemoteShard struct {
	Shard
	servers []*ClusterServer
}

type LocalShard struct {
	Shard
	store LocalShardStore
}

type LocalShardDb interface {
	Write([]*protocol.Series) error
	Query(*parser.Query, chan *protocol.Response) error
}

type LocalShardStore interface {
	GetOrCreateShard(id uint32) (LocalShardDb, error)
}

func (self *Shard) Id() uint32 {
	return self.id
}

func (self *Shard) StartTime() time.Time {
	return self.startTime
}
func (self *Shard) EndTime() time.Time {
	return self.endTime
}

func (self *LocalShard) Write([]*protocol.Series) error {
	return nil
}

func (self *LocalShard) Query(*parser.Query, chan *protocol.Response) error {
	return nil
}

func (self *RemoteShard) Write([]*protocol.Series) error {
	return nil
}

func (self *RemoteShard) Query(*parser.Query, chan *protocol.Response) error {
	return nil
}
