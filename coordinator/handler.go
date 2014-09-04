package coordinator

import (
	"net"

	"github.com/influxdb/influxdb/protocol"
)

type Handler interface {
	HandleRequest(*protocol.Request, net.Conn) error
}
