package graphite

import (
	"bufio"
	"log"
	"net"
	"os"
	"strings"
	"sync"

	"github.com/influxdb/influxdb"
)

// TCPServer processes Graphite data received over TCP connections.
type TCPServer struct {
	writer   SeriesWriter
	parser   *Parser
	database string
	listener *net.Listener

	wg sync.WaitGroup

	Logger *log.Logger

	host string
	mu   sync.Mutex
}

// NewTCPServer returns a new instance of a TCPServer.
func NewTCPServer(p *Parser, w SeriesWriter, db string) *TCPServer {
	return &TCPServer{
		parser:   p,
		writer:   w,
		database: db,
		Logger:   log.New(os.Stderr, "[graphite] ", log.LstdFlags),
	}
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
	t.listener = &ln
	t.mu.Lock()
	t.host = ln.Addr().String()
	t.mu.Unlock()

	t.Logger.Println("listening on TCP connection", ln.Addr().String())
	t.wg.Add(1)
	go func() {
		defer t.wg.Done()
		for {
			conn, err := ln.Accept()
			if opErr, ok := err.(*net.OpError); ok && !opErr.Temporary() {
				t.Logger.Println("graphite TCP listener closed")
				return
			}
			if err != nil {
				t.Logger.Println("error accepting TCP connection", err.Error())
				continue
			}

			t.wg.Add(1)
			go t.handleConnection(conn)
		}
	}()
	return nil
}

func (t *TCPServer) Host() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.host
}

func (t *TCPServer) Close() error {
	var err error
	if t.listener != nil {
		err = (*t.listener).Close()
	}
	t.wg.Wait()
	t.listener = nil
	return err
}

// handleConnection services an individual TCP connection.
func (t *TCPServer) handleConnection(conn net.Conn) {
	defer conn.Close()
	defer t.wg.Done()

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
