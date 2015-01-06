package graphite

import (
	"bufio"
	"io"
	"log"
	"net"
	"strings"
)

const (
	udpBufferSize = 65536
)

// UdpGraphiteServer processes Graphite data received via UDP packets.
type UdpGraphiteServer struct {
	GrpahiteServer
}

// NewUdpGraphiteServer returns a new instance of a UdpGraphiteServer.
func NewUdpGraphiteServer(d dataSink) *UdpGraphiteServer {
	u := UdpGraphiteServer{}
	u.sink = d
	return &u
}

// Start instructs the UdpGraphiteServer to start processing Graphite data
// on the given interface. iface must be in the form host:port
func (u *UdpGraphiteServer) Start(iface string) error {
	if iface == "" { // Make sure we have an address
		return ErrBindAddressRequired
	} else if u.Database == "" { // Make sure they have a database
		return ErrDatabaseNotSpecified
	} else if u.sink == nil { // Make sure they specified a backend sink
		return ErrServerNotSpecified
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
				if err == io.EOF {
					return
				} else {
					log.Println("ignoring error reading Graphite data over UDP", err.Error())
				}
			}
			for _, metric := range strings.Split(string(buf[:n]), "\n") {
				u.handleMessage(bufio.NewReader(strings.NewReader(metric + "\n")))
			}
		}
	}()
	return nil
}
