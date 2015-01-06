package graphite

import (
	"bufio"
	"io"
	"log"
	"net"
)

// TcpGraphiteServer processes Graphite data received over TCP connections.
type TcpGraphiteServer struct {
	GraphiteServer
}

// NewTcpGraphiteServer returns a new instance of a TcpGraphiteServer.
func NewTcpGraphiteServer(d dataSink) *TcpGraphiteServer {
	t := TcpGraphiteServer{}
	t.sink = d
	return &t
}

// Start instructs the TcpGraphiteServer to start processing Graphite data
// on the given interface. iface must be in the form host:port
func (t *TcpGraphiteServer) Start(iface string) error {
	if iface == "" { // Make sure we have an address
		return ErrBindAddressRequired
	} else if t.Database == "" { // Make sure they have a database
		return ErrDatabaseNotSpecified
	} else if t.sink == nil { // Make sure they specified a backend sink
		return ErrServerNotSpecified
	}

	ln, err := net.Listen("tcp", iface)
	if err != nil {
		return err
	}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				log.Println("erorr accepting TCP connection", err.Error())
				continue
			}
			go t.handleConnection(conn)
		}
	}()
	return nil
}
func (t *TcpGraphiteServer) handleConnection(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	for {
		err := t.handleMessage(reader)
		if err != nil {
			if err == io.EOF {
				return
			} else {
				log.Println("ignoring error reading graphite data over TCP", err.Error())
			}
		}
	}
}
