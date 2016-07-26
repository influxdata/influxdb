package admin // import "github.com/influxdata/influxdb/services/admin"

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"strings"

	// Register static assets via statik.
	_ "github.com/influxdata/influxdb/services/admin/statik"
	"github.com/influxdata/log"
	"github.com/rakyll/statik/fs"
)

// Service manages the listener for an admin endpoint.
type Service struct {
	listener net.Listener
	addr     string
	https    bool
	cert     string
	err      chan error
	version  string

	logger *log.Logger
}

// NewService returns a new instance of Service.
func NewService(c Config) *Service {
	s := &Service{
		addr:    c.BindAddress,
		https:   c.HTTPSEnabled,
		cert:    c.HTTPSCertificate,
		err:     make(chan error),
		version: c.Version,
	}
	s.WithLogger(log.Log)
	return s
}

// Open starts the service
func (s *Service) Open() error {
	s.logger.Info("Starting admin service")

	// Open listener.
	if s.https {
		cert, err := tls.LoadX509KeyPair(s.cert, s.cert)
		if err != nil {
			return err
		}

		listener, err := tls.Listen("tcp", s.addr, &tls.Config{
			Certificates: []tls.Certificate{cert},
		})
		if err != nil {
			return err
		}

		s.logger.Infof("Listening on HTTPS: %s", listener.Addr().String())
		s.listener = listener
	} else {
		listener, err := net.Listen("tcp", s.addr)
		if err != nil {
			return err
		}

		s.logger.Infof("Listening on HTTP: %s", listener.Addr().String())
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

// WithLogger sets the logger to augment for log messages. It must not be
// called after the Open method has been called.
func (s *Service) WithLogger(l *log.Logger) {
	s.logger = l.WithField("service", "admin")
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
	addVersionHeaderThenServe := func(h http.Handler) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			w.Header().Add("X-InfluxDB-Version", s.version)
			h.ServeHTTP(w, r)
		}
	}

	// Instantiate file system from embedded admin.
	statikFS, err := fs.New()
	if err != nil {
		panic(err)
	}

	// Run file system handler on listener.
	err = http.Serve(s.listener, addVersionHeaderThenServe(http.FileServer(statikFS)))
	if err != nil && !strings.Contains(err.Error(), "closed") {
		s.err <- fmt.Errorf("listener error: addr=%s, err=%s", s.Addr(), err)
	}
}
