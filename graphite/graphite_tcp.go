package graphite

import (
	"bufio"
	"io"
	"log"
	"net"
	"strings"

	"github.com/influxdb/influxdb"
)

// TCPServer processes Graphite data received over TCP connections.
type TCPServer struct {
	writer   SeriesWriter
	parser   *Parser
	database string

	Logger *log.Logger
}

// NewTCPServer returns a new instance of a TCPServer.
func NewTCPServer(p *Parser, w SeriesWriter, db string) *TCPServer {
	return &TCPServer{
		parser:   p,
		writer:   w,
		database: db,
	}
}

// SetLogOutput sets writer for all Graphite log output.
func (s *TCPServer) SetLogOutput(w io.Writer) {
	s.Logger = log.New(w, "[graphite] ", log.LstdFlags)
}

// ListenAndServe instructs the TCPServer to start processing Graphite data
// on the given interface. iface must be in the form host:port
func (t *TCPServer) ListenAndServe(iface string) error {
	if iface == "" { // Make sure we have an address
		return ErrBindAddressRequired
	}

	ln, err := net.Listen("tcp", iface)
	if err != nil {
		return err
	}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				t.Logger.Println("error accepting TCP connection", err.Error())
				continue
			}
			go t.handleConnection(conn)
		}
	}()
	return nil
}

// handleConnection services an individual TCP connection.
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
		point, err := t.parser.Parse(line)
		if err != nil {
			t.Logger.Printf("unable to parse data: %s", err)
			continue
		}

		// Send the data to the writer.
		_, e := t.writer.WriteSeries(t.database, "", []influxdb.Point{point})
		if e != nil {
			t.Logger.Printf("failed to write data point to database %q: %s\n", t.database, e)
		}
	}
}
