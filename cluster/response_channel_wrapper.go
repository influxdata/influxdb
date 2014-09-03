package cluster

import (
	"code.google.com/p/log4go"
	"github.com/influxdb/influxdb/protocol"
)

// A `ResponseProcessor' that wraps a go channel.
type ResponseChannelWrapper struct {
	c chan<- *protocol.Response
}

func NewResponseChannelWrapper(c chan<- *protocol.Response) ResponseChannel {
	return &ResponseChannelWrapper{c}
}

func (w *ResponseChannelWrapper) Yield(r *protocol.Response) bool {
	log4go.Debug("ResponseChannelWrapper: Yielding %s", r)
	w.c <- r
	return true
}

func (w *ResponseChannelWrapper) Name() string {
	return "ResponseChannelWrapper"
}
