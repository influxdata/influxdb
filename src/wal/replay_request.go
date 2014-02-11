package wal

import (
	"protocol"
)

type replayRequest struct {
	request *protocol.Request
	shardId uint32
	err     error
}
