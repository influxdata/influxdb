package subscriber

import (
	"net"

	"github.com/influxdb/influxdb/cluster"
)

// Writes points over UDP using the line protocol
type UDP struct {
	addr string
}

func NewUDP(addr string) *UDP {
	return &UDP{addr: addr}
}

func (u *UDP) WritePoints(p *cluster.WritePointsRequest) (err error) {
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

	for _, p := range p.Points {
		_, err = con.Write([]byte(p.String()))
		if err != nil {
			return
		}

	}
	return
}
