package wal

import (
	"protocol"
)

type replayRequest struct {
	requestNumber uint32
	request       *protocol.Request
	shardId       uint32
	err           error
}

func newErrorReplayRequest(err error) *replayRequest {
	return &replayRequest{
		err: err,
	}
}
