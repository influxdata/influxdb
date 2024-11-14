package subscriber

import (
	"context"
	"net"
	"net/url"

	errors2 "github.com/influxdata/influxdb/pkg/errors"
)

// UDP supports writing points over UDP using the line protocol.
type UDP struct {
	addr        string
	destination string
}

// NewUDP returns a new UDP listener with default options.
func NewUDP(u url.URL) *UDP {
	return &UDP{addr: u.Host, destination: u.String()}
}

// WritePoints writes points over UDP transport.
func (u *UDP) WritePointsContext(_ context.Context, request WriteRequest) (destination string, err error) {
	var addr *net.UDPAddr
	var con *net.UDPConn

	destination = u.destination
	addr, err = net.ResolveUDPAddr("udp", u.addr)
	if err != nil {
		return
	}

	con, err = net.DialUDP("udp", nil, addr)
	if err != nil {
		return
	}
	defer errors2.Capture(&err, con.Close)()

	for i := range request.pointOffsets {
		// write the point without the trailing newline
		pointRaw := request.PointAt(i)
		_, err = con.Write(pointRaw[:pointRaw[len(pointRaw)-1]])
		if err != nil {
			return
		}
	}
	return
}
