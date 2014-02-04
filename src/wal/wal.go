package wal

import (
	"configuration"
	"protocol"
	"sync/atomic"
)

type WAL struct {
	config             *configuration.Configuration
	lastSequenceNumber uint64
	serverId           uint64
}

const HOST_ID_OFFSET = uint64(10000)

func NewWAL(config *configuration.Configuration, serverId uint32) (*WAL, error) {
	return &WAL{config: config, serverId: uint64(serverId)}, nil
}

type Shard interface {
	Id() uint32
}

type Server interface {
	Id() uint32
}

// Will assign sequence numbers if null. Returns a unique id that should be marked as committed for each server
// as it gets confirmed.
func (self *WAL) AssignSequenceNumbersAndLog(request *protocol.Request, shard Shard, servers []Server) (uint64, error) {
	for _, point := range request.Series.Points {
		if point.SequenceNumber == nil {
			sn := self.getNextSequenceNumber(shard)
			point.SequenceNumber = &sn
		}
	}

	// TODO: assign a unique number and actually log it
	return uint64(1), nil
}

// Marks a given request for a given server as committed
func (self *WAL) Commit(requestNumber uint64, server Server) error {
	// TODO: make this do sometihing
	return nil
}

// Yields to the passed in function requests that need to be sent back out to the associated server. If the yield function
// returns a nil, the request is marked as committed. Should also make sure that any sequence numbers aren't repeated from
// the log (even those that weren't committed)
func (self *WAL) RecoverFromLog(yield func(request *protocol.Request, shard Shard, server Server) error) error {
	// TODO: make this do stuff
	return nil
}

// In the case where this server is running and another one in the cluster stops responding, at some point this server will have to just write
// requests to disk. When the downed server comes back up, it's this server's responsibility to send out any writes that were queued up. If
// the yield function returns nil then the request is committed.
func (self *WAL) RecoverServerFromRequestNumber(requestNumber uint64, server Server, yield func(request *protocol.Request, shard Shard) error) error {
	// TODO: make this do stuff
	return nil
}

// TODO: make this persistent. could scope sequence numbers by shard.
func (self *WAL) getNextSequenceNumber(shard Shard) uint64 {
	return atomic.AddUint64(&self.lastSequenceNumber, uint64(1))*HOST_ID_OFFSET + self.serverId
}
