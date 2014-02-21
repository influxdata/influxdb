package wal

import (
	"protocol"
)

type closeEntry struct {
	confirmation chan *confirmation
}

type commitEntry struct {
	confirmation  chan *confirmation
	serverId      uint32
	requestNumber uint32
}

type appendEntry struct {
	confirmation chan *confirmation
	request      *protocol.Request
	shardId      uint32
}
