package cluster

import "github.com/influxdb/influxdb/protocol"

// ResponseChannel is a processor for Responses as opposed to Series
// like `engine.Processor'
type ResponseChannel interface {
	Yield(r *protocol.Response) bool
	Name() string
}
