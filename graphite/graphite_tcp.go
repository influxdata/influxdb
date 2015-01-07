package graphite

import (
	"bufio"
	"log"
	"net"
	"strings"
)

// TCPServer processes Graphite data received over TCP connections.
type TCPServer struct {
	writer SeriesWriter
	parser *Parser

	Database string
}

// NewTCPServer returns a new instance of a TCPServer.
func NewTCPServer(p *Parser, w SeriesWriter) *TCPServer {
	t := TCPServer{
		parser: p,
		writer: w,
	}
	return &t
}

// ListenAndServe instructs the TCPServer to start processing Graphite data
// on the given interface. iface must be in the form host:port
func (t *TCPServer) ListenAndServe(iface string) error {
	if iface == "" { // Make sure we have an address
		return ErrBindAddressRequired
	} else if t.Database == "" { // Make sure they have a database
		return ErrDatabaseNotSpecified
	}

	ln, err := net.Listen("tcp", iface)
	if err != nil {
		return err
	}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				log.Println("error accepting TCP connection", err.Error())
				continue
			}
			go t.handleConnection(conn)
		}
	}()
	return nil
}
func (t *TCPServer) handleConnection(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	for {
		// Read up to the next newline.
		buf, err := reader.ReadBytes('\n')
		if err != nil {
			return
		}

		// Trim the buffer, even though there should be no padding
		line := strings.TrimSpace(string(buf))

		// Parse it.
		metric, err := t.parser.Parse(line)
		if err != nil {
			continue
		}

		// Convert metric to a field value.
		var values = make(map[string]interface{})
		values[metric.Name] = metric.Value

		// Send the data to database
		t.writer.WriteSeries(t.Database, "", metric.Name, metric.Tags, metric.Timestamp, values)
	}
}
