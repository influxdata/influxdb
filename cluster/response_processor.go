package cluster

import "github.com/influxdb/influxdb/protocol"

type ResponseChannel interface {
	Yield(r *protocol.Response) bool
}
