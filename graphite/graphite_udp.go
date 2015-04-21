package graphite

import (
	"log"
	"net"
	"os"
	"strings"
	"sync"

	"github.com/influxdb/influxdb"
)

const (
	udpBufferSize = 65536
)

// UDPServer processes Graphite data received via UDP.
type UDPServer struct {
	writer   SeriesWriter
	parser   *Parser
	database string
	conn     *net.UDPConn
	addr     *net.UDPAddr
	wg       sync.WaitGroup

	Logger *log.Logger

	host string
}

// NewUDPServer returns a new instance of a UDPServer
func NewUDPServer(p *Parser, w SeriesWriter, db string) *UDPServer {
	u := UDPServer{
		parser:   p,
		writer:   w,
		database: db,
		Logger:   log.New(os.Stderr, "[graphite] ", log.LstdFlags),
	}
	return &u
}

// ListenAndServer instructs the UDPServer to start processing Graphite data
// on the given interface. iface must be in the form host:port.
func (u *UDPServer) ListenAndServe(iface string) error {
	if iface == "" { // Make sure we have an address
		return ErrBindAddressRequired
	}

	addr, err := net.ResolveUDPAddr("udp", iface)
	if err != nil {
		return err
	}

	u.addr = addr

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return err
	}

	u.host = u.addr.String()

	buf := make([]byte, udpBufferSize)
	u.wg.Add(1)
	go func() {
		defer u.wg.Done()
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

				// Send the data to the writer.
				_, e := u.writer.WriteSeries(u.database, "", []influxdb.Point{point})
				if e != nil {
					u.Logger.Printf("failed to write data point: %s\n", e)
				}
			}
		}
	}()
	return nil
}

func (u *UDPServer) Host() string {
	return u.host
}

func (u *UDPServer) Close() error {
	err := u.Close()
	u.wg.Done()
	return err
}
