package wal

import (
	"configuration"
	"fmt"
	"os"
	"path"
	"protocol"
)

type WAL struct {
	config *configuration.Configuration
	log    *log
}

const HOST_ID_OFFSET = uint64(10000)

func NewWAL(config *configuration.Configuration) (*WAL, error) {
	if config.WalDir == "" {
		return nil, fmt.Errorf("wal directory cannot be empty")
	}
	_, err := os.Stat(config.WalDir)

	if os.IsNotExist(err) {
		err = os.MkdirAll(config.WalDir, 0755)
	}

	if err != nil {
		return nil, err
	}

	logFile, err := os.OpenFile(path.Join(config.WalDir, "log"), os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	log, err := newLog(logFile)
	if err != nil {
		return nil, err
	}

	return &WAL{config: config, log: log}, nil
}

type Shard interface {
	Id() uint32
}

type Server interface {
	Id() uint32
}

func (self *WAL) SetServerId(id uint32) {
	self.log.setServerId(id)
}

// Will assign sequence numbers if null. Returns a unique id that should be marked as committed for each server
// as it gets confirmed.
func (self *WAL) AssignSequenceNumbersAndLog(request *protocol.Request, shard Shard, servers []Server) (uint32, error) {
	return self.log.appendRequest(request, shard.Id())
}

// Marks a given request for a given server as committed
func (self *WAL) Commit(requestNumber uint32, server Server) error {
	return fmt.Errorf("not implemented yet")
}

// In the case where this server is running and another one in the cluster stops responding, at some point this server will have to just write
// requests to disk. When the downed server comes back up, it's this server's responsibility to send out any writes that were queued up. If
// the yield function returns nil then the request is committed.
func (self *WAL) RecoverServerFromRequestNumber(requestNumber uint32, shardIds []uint32, yield func(request *protocol.Request, shardId uint32) error) error {
	ch, stopChan := self.log.replayFromRequestNumber(shardIds, requestNumber)
	for {
		x := <-ch
		if x == nil {
			return nil
		}

		if x.err != nil {
			return x.err
		}

		if err := yield(x.request, x.shardId); err != nil {
			stopChan <- struct{}{}
			return err
		}
	}
}
