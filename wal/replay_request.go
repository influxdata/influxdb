package wal

import (
	"github.com/influxdb/influxdb/common"
	"github.com/influxdb/influxdb/protocol"
)

type replayRequest struct {
	requestNumber uint32
	request       *protocol.Request
	shardId       uint32
	startOffset   int64
	endOffset     int64
	err           error
}

func newErrorReplayRequest(err error) *replayRequest {
	return &replayRequest{
		err: common.NewErrorWithStacktrace(err, "Replay error"),
	}
}
