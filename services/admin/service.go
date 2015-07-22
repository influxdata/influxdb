package admin

import (
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strings"

	// Register static assets via statik.
	_ "github.com/influxdb/influxdb/statik"
	"github.com/rakyll/statik/fs"
)

// Service manages the listener for an admin endpoint.
type Service struct {
	listener net.Listener
	addr     string
	https    bool
	cert     string
	err      chan error

	logger *log.Logger
}

// NewService returns a new instance of Service.
func NewService(c Config) *Service {
	return &Service{
		addr:   c.BindAddress,
		https:  c.HttpsEnabled,
		cert:   c.HttpsCertificate,
		err:    make(chan error),
		logger: log.New(os.Stderr, "[admin] ", log.LstdFlags),
	}
}

// Open starts the service
func (s *Service) Open() error {
	// Open listener.
	if s.https {
		cert, err := tls.LoadX509KeyPair(s.cert, s.cert)
		if err != nil {
			s.logger.Printf("Failed to load HTTPS certificate '%s': %s", s.cert, err)
			s.logger.Println("Reverting to HTTP")
		}

		listener, err := tls.Listen("tcp", s.addr, &tls.Config{
			Certificates: []tls.Certificate{cert},
		})
		if err != nil {
			s.logger.Printf("Failed to bind HTTPS on %s: %s", s.addr, err)
			s.logger.Println("Reverting to HTTP")
		}

		s.logger.Println("listening on HTTPS:", listener.Addr().String())
		s.listener = listener
	}

	// Assume that the HTTPS configuration failed
	if s.listener == nil {
		listener, err := net.Listen("tcp", s.addr)
		if err != nil {
			return err
		}

		s.logger.Println("listening on HTTP:", listener.Addr().String())
		s.listener = listener
	}

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

// SetLogger sets the internal logger to the logger passed in.
func (s *Service) SetLogger(l *log.Logger) {
	s.logger = l
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
