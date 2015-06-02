package admin

import (
	"fmt"
	"net"
	"net/http"
	"strings"

	// Register static assets via statik.
	_ "github.com/influxdb/influxdb/statik"
	"github.com/rakyll/statik/fs"
)

// Service manages the listener for an admin endpoint.
type Service struct {
	listener net.Listener
	addr     string
	err      chan error
}

// NewService returns a new instance of Service.
func NewService(c Config) *Service {
	return &Service{
		addr: c.BindAddress,
		err:  make(chan error),
	}
}

// Open starts the service
func (s *Service) Open() error {
	// Open listener.
	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.listener = listener

	// Begin listening for requests in a separate goroutine.
	go s.serve()
	return nil
}

// Close closes the underlying listener.
func (s *Service) Close() error {
	if s.listener != nil {
		return s.listener.Close()
	}
	return nil
}

// Err returns a channel for fatal errors that occur on the listener.
func (s *Service) Err() <-chan error { return s.err }

// Addr returns the listener's address. Returns nil if listener is closed.
func (s *Service) Addr() net.Addr {
	if s.listener != nil {
		return s.listener.Addr()
	}
	return nil
}

// serve serves the handler from the listener.
func (s *Service) serve() {
	// Instantiate file system from embedded admin.
	statikFS, err := fs.New()
	if err != nil {
		panic(err)
	}

	// Run file system handler on listener.
	err = http.Serve(s.listener, http.FileServer(statikFS))
	if err != nil && !strings.Contains(err.Error(), "closed") {
		s.err <- fmt.Errorf("listener error: addr=%s, err=%s", s.Addr(), err)
	}
}
