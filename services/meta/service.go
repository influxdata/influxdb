package meta // import "github.com/influxdata/influxdb/services/meta"

import (
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"time"
)

const (
	MuxHeader = 8
)

type Service struct {
	RaftListener net.Listener

	config   *Config
	handler  *handler
	ln       net.Listener
	httpAddr string
	raftAddr string
	https    bool
	cert     string
	err      chan error
	Logger   *log.Logger
	store    *store
}

// NewService returns a new instance of Service.
func NewService(c *Config) *Service {
	s := &Service{
		config:   c,
		httpAddr: c.DefaultedHTTPBindAddress(),
		raftAddr: c.DefaultedBindAddress(),
		https:    c.HTTPSEnabled,
		cert:     c.HTTPSCertificate,
		err:      make(chan error),
	}
	if c.LoggingEnabled {
		s.Logger = log.New(os.Stderr, "[meta] ", log.LstdFlags)
	} else {
		s.Logger = log.New(ioutil.Discard, "", 0)
	}

	return s
}

// Open starts the service
func (s *Service) Open() error {
	s.Logger.Println("Starting meta service")

	if s.RaftListener == nil {
		panic("no raft listener set")
	}

	// Open listener.
	if s.https {
		cert, err := tls.LoadX509KeyPair(s.cert, s.cert)
		if err != nil {
			return err
		}

		listener, err := tls.Listen("tcp", s.httpAddr, &tls.Config{
			Certificates: []tls.Certificate{cert},
		})
		if err != nil {
			return err
		}

		s.Logger.Println("Listening on HTTPS:", listener.Addr().String())
		s.ln = listener
	} else {
		listener, err := net.Listen("tcp", s.httpAddr)
		if err != nil {
			return err
		}

		s.Logger.Println("Listening on HTTP:", listener.Addr().String())
		s.ln = listener
	}

	// wait for the listeners to start
	timeout := time.Now().Add(raftListenerStartupTimeout)
	for {
		if s.ln.Addr() != nil && s.RaftListener.Addr() != nil {
			break
		}

		if time.Now().After(timeout) {
			return fmt.Errorf("unable to open without http listener running")
		}
		time.Sleep(10 * time.Millisecond)
	}

	var err error
	if autoAssignPort(s.httpAddr) {
		s.httpAddr, err = combineHostAndAssignedPort(s.ln, s.httpAddr)
	}
	if autoAssignPort(s.raftAddr) {
		s.raftAddr, err = combineHostAndAssignedPort(s.RaftListener, s.raftAddr)
	}
	if err != nil {
		return err
	}

	// Open the store
	s.store = newStore(s.config, s.httpAddr, s.raftAddr)

	handler := newHandler(s.config, s)
	handler.logger = s.Logger
	handler.store = s.store
	s.handler = handler

	// Begin listening for requests in a separate goroutine.
	go s.serve()

	if err := s.store.open(s.RaftListener); err != nil {
		return err
	}

	return nil
}

// serve serves the handler from the listener.
func (s *Service) serve() {
	// The listener was closed so exit
	// See https://github.com/golang/go/issues/4373
	err := http.Serve(s.ln, s.handler)
	if err != nil && !strings.Contains(err.Error(), "closed") {
		s.err <- fmt.Errorf("listener failed: addr=%s, err=%s", s.ln.Addr(), err)
	}
}

// Close closes the underlying listener.
func (s *Service) Close() error {
	if err := s.handler.Close(); err != nil {
		return err
	}

	if err := s.store.close(); err != nil {
		return err
	}

	if s.ln != nil {
		if err := s.ln.Close(); err != nil {
			return err
		}
	}

	return nil
}

// HTTPAddr returns the bind address for the HTTP API
func (s *Service) HTTPAddr() string {
	return s.httpAddr
}

// RaftAddr returns the bind address for the Raft TCP listener
func (s *Service) RaftAddr() string {
	return s.raftAddr
}

// Err returns a channel for fatal errors that occur on the listener.
func (s *Service) Err() <-chan error { return s.err }

// SetLogger sets the internal logger to the logger passed in.
func (s *Service) SetLogger(l *log.Logger) {
	s.Logger = l
}

func autoAssignPort(addr string) bool {
	_, p, _ := net.SplitHostPort(addr)
	return p == "0"
}

func combineHostAndAssignedPort(ln net.Listener, autoAddr string) (string, error) {
	host, _, err := net.SplitHostPort(autoAddr)
	if err != nil {
		return "", err
	}
	_, port, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		return "", err
	}
	return net.JoinHostPort(host, port), nil
}
