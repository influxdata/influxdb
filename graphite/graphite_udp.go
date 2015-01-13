package graphite

import (
	"net"
	"strings"

	"github.com/influxdb/influxdb"
)

const (
	udpBufferSize = 65536
)

// UDPerver processes Graphite data received via UDP.
type UDPServer struct {
	writer SeriesWriter
	parser *Parser

	Database string
}

// NewUDPServer returns a new instance of a UDPServer
func NewUDPServer(p *Parser, w SeriesWriter) *UDPServer {
	u := UDPServer{
		parser: p,
		writer: w,
	}
	return &u
}

// ListenAndServer instructs the UDPServer to start processing Graphite data
// on the given interface. iface must be in the form host:port.
func (u *UDPServer) ListenAndServe(iface string) error {
	if iface == "" { // Make sure we have an address
		return ErrBindAddressRequired
	} else if u.Database == "" { // Make sure they have a database
		return ErrDatabaseNotSpecified
	}

	addr, err := net.ResolveUDPAddr("udp", iface)
	if err != nil {
		return nil
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return err
	}

	buf := make([]byte, udpBufferSize)
	go func() {
		for {
			n, _, err := conn.ReadFromUDP(buf)
			if err != nil {
				return
			}
			for _, line := range strings.Split(string(buf[:n]), "\n") {
				point, err := u.parser.Parse(line)
				if err != nil {
					continue
				}

				// Send the data to database
				u.writer.WriteSeries(u.Database, "", []influxdb.Point{point})
			}
		}
	}()
	return nil
}
