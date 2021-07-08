package subscriber

import (
	"context"
	"net"
)

// UDP supports writing points over UDP using the line protocol.
type UDP struct {
	addr string
}

// NewUDP returns a new UDP listener with default options.
func NewUDP(addr string) *UDP {
	return &UDP{addr: addr}
}

// WritePoints writes points over UDP transport.
func (u *UDP) WritePointsContext(_ context.Context, request WriteRequest) (err error) {
	var addr *net.UDPAddr
	var con *net.UDPConn
	addr, err = net.ResolveUDPAddr("udp", u.addr)
	if err != nil {
		return
	}

	con, err = net.DialUDP("udp", nil, addr)
	if err != nil {
		return
	}
	defer con.Close()

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
