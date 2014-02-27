package wal

import (
	"common"
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
		err: common.NewErrorWithStacktrace(err, "Replay error"),
	}
}
