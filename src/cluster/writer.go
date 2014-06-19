package cluster

import "protocol"

type Writer interface {
	Write(request *protocol.Request) error
}
