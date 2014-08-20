package cluster

import "github.com/influxdb/influxdb/protocol"

type ResponseChannelWrapper struct {
	c chan<- *protocol.Response
}

func NewResponseChannelWrapper(c chan<- *protocol.Response) ResponseChannel {
	return &ResponseChannelWrapper{c}
}

func (w *ResponseChannelWrapper) Yield(r *protocol.Response) bool {
	w.c <- r
	return true
}
