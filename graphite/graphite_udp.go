package graphite

import (
	"io"
	"log"
	"net"
	"strings"

	"github.com/influxdb/influxdb"
)

const (
	udpBufferSize = 65536
)

// UDPServer processes Graphite data received via UDP.
type UDPServer struct {
	server SeriesWriter
	parser *Parser

	Database string
	Logger   *log.Logger
}

// NewUDPServer returns a new instance of a UDPServer
func NewUDPServer(p *Parser, s SeriesWriter) *UDPServer {
	u := UDPServer{
		parser: p,
		server: s,
	}
	return &u
}

// SetLogOutput sets writer for all Graphite log output.
func (s *UDPServer) SetLogOutput(w io.Writer) {
	s.Logger = log.New(w, "[graphite] ", log.LstdFlags)
}

// SetDatabase sets the database for all Graphite log output.
func (s *UDPServer) SetDatabase(database string) {
	s.Database = database
}

// Protocol returns a string version of the supported protocol.
func (s *UDPServer) Protocol() string {
	return "udp"
}

// ListenAndServer instructs the UDPServer to start processing Graphite data
// on the given interface. iface must be in the form host:port.
func (u *UDPServer) ListenAndServe(iface string) error {
	if iface == "" { // Make sure we have an address
		return ErrBindAddressRequired
	} else if u.Database == "" { // Make sure they have a database
		// If they didn't specify a database, create one and set a default retention policy.
		if !u.server.DatabaseExists(DefaultDatabaseName) {
			u.Logger.Printf("default database %q does not exist.  creating.\n", DefaultDatabaseName)
			if e := u.server.CreateDatabase(DefaultDatabaseName); e != nil {
				return e
			}
			u.Database = DefaultDatabaseName
		}
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
				_, e := u.server.WriteSeries(u.Database, "", []influxdb.Point{point})
				if e != nil {
					u.Logger.Printf("failed to write data point: %s\n", e)
				}
			}
		}
	}()
	return nil
}
