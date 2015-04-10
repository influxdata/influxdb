package admin

import (
	"log"
	"net"
	"net/http"
	"strings"
	"sync"

	"github.com/rakyll/statik/fs"

	// Register static assets via statik.
	_ "github.com/influxdb/influxdb/statik"
)

// Server manages InfluxDB's admin web server.
type Server struct {
	mu       sync.Mutex
	addr     string
	listener net.Listener
	closed   bool
}

// NewServer constructs a new admin web server. The "addr" argument should be a
// string that looks like ":8083" or whatever addr to serve on.
func NewServer(addr string) *Server {
	return &Server{addr: addr, closed: true}
}

// ListenAndServe starts the admin web server and serves requests until
// s.Close() is called.
func (s *Server) ListenAndServe() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.addr == "" {
		return nil
	}

	var err error
	s.listener, err = net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}

	s.closed = false
	statikFS, _ := fs.New()

	go func() {
		err = http.Serve(s.listener, http.FileServer(statikFS))
		if !strings.Contains(err.Error(), "closed") {
			log.Fatalf("admin server failed to server on %s: %s", s.addr, err)
		}
	}()
	return err
}

// Close stops the admin web server.
func (s *Server) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}

	s.closed = true
	return s.listener.Close()
}
