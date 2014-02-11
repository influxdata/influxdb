package wal

import (
	"protocol"
)

type entry struct {
	confirmation chan *confirmation
	request      *protocol.Request
	shardId      uint32
}
