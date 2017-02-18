package admin // import "github.com/influxdata/influxdb/services/admin"

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"strings"

	// Register static assets via statik.
	_ "github.com/influxdata/influxdb/services/admin/statik"
	"github.com/rakyll/statik/fs"
	"github.com/uber-go/zap"
)

// Service manages the listener for an admin endpoint.
type Service struct {
	listener net.Listener
	addr     string
	https    bool
	cert     string
	err      chan error
	version  string

	logger zap.Logger
}

// NewService returns a new instance of Service.
func NewService(c Config) *Service {
	return &Service{
		addr:    c.BindAddress,
		https:   c.HTTPSEnabled,
		cert:    c.HTTPSCertificate,
		err:     make(chan error),
		version: c.Version,
		logger:  zap.New(zap.NullEncoder()),
	}
}

// Open starts the service
func (s *Service) Open() error {
	s.logger.Info("Starting admin service")
	s.logger.Info("DEPRECATED: This plugin is deprecated as of 1.1.0 and will be removed in a future release")

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

		s.logger.Info(fmt.Sprint("Listening on HTTPS: ", listener.Addr().String()))
		s.listener = listener
	} else {
		listener, err := net.Listen("tcp", s.addr)
		if err != nil {
			return err
		}

		s.logger.Info(fmt.Sprint("Listening on HTTP: ", listener.Addr().String()))
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

func (s *Service) WithLogger(log zap.Logger) {
	s.logger = log.With(zap.String("service", "admin"))
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
