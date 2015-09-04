package httpd

import (
	"crypto/tls"
	"expvar"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/influxdb/influxdb/monitor"
)

// statistics gathered by the httpd package.
const (
	statRequests           = "requests"
	statCQServed           = "cq_served"
	statQueriesServed      = "queries_served"
	statWriteServed        = "write_served"
	statPingServed         = "ping_served"
	statWriteBytesReceived = "write_bytes_rx"
	statPointsTransmitted  = "points_tx"
	statPointsTransmitFail = "points_tx_fail"
	statAuthFail           = "auth_fail"
)

// Service manages the listener and handler for an HTTP endpoint.
type Service struct {
	ln      net.Listener
	addr    string
	https   bool
	cert    string
	err     chan error
	statMap *expvar.Map

	Handler *Handler

	Monitor interface {
		RegisterStatsClient(name string, tags map[string]string, client monitor.StatsClient) error
	}

	Logger *log.Logger
}

// NewService returns a new instance of Service.
func NewService(c Config) *Service {
	serviceStatMap := expvarStatMap(c.BindAddress)

	s := &Service{
		addr:    c.BindAddress,
		https:   c.HttpsEnabled,
		cert:    c.HttpsCertificate,
		err:     make(chan error),
		statMap: serviceStatMap,
		Handler: NewHandler(
			c.AuthEnabled,
			c.LogEnabled,
			c.WriteTracing,
			serviceStatMap,
		),
		Logger: log.New(os.Stderr, "[httpd] ", log.LstdFlags),
	}
	s.Handler.Logger = s.Logger
	return s
}

// Open starts the service
func (s *Service) Open() error {
	s.Logger.Println("Starting HTTP service")
	s.Logger.Println("Authentication enabled:", s.Handler.requireAuthentication)

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

		s.Logger.Println("Listening on HTTPS:", listener.Addr().String())
		s.ln = listener
	} else {
		listener, err := net.Listen("tcp", s.addr)
		if err != nil {
			return err
		}

		s.Logger.Println("Listening on HTTP:", listener.Addr().String())
		s.ln = listener
	}

	// Register stats for this service, now that it has started successfully.
	if s.Monitor != nil {
		t := monitor.NewStatsMonitorClient(s.statMap)
		s.Monitor.RegisterStatsClient("httpd", map[string]string{"bind": s.addr}, t)
	}

	// Begin listening for requests in a separate goroutine.
	go s.serve()
	return nil
}

// Close closes the underlying listener.
func (s *Service) Close() error {
	if s.ln != nil {
		return s.ln.Close()
	}
	return nil
}

// SetLogger sets the internal logger to the logger passed in.
func (s *Service) SetLogger(l *log.Logger) {
	s.Logger = l
}

// Err returns a channel for fatal errors that occur on the listener.
func (s *Service) Err() <-chan error { return s.err }

// Addr returns the listener's address. Returns nil if listener is closed.
func (s *Service) Addr() net.Addr {
	if s.ln != nil {
		return s.ln.Addr()
	}
	return nil
}

// serve serves the handler from the listener.
func (s *Service) serve() {
	// The listener was closed so exit
	// See https://github.com/golang/go/issues/4373
	err := http.Serve(s.ln, s.Handler)
	if err != nil && !strings.Contains(err.Error(), "closed") {
		s.err <- fmt.Errorf("listener failed: addr=%s, err=%s", s.Addr(), err)
	}
}

// expvarStatMap returns the expvar based collection for this service. It must be done within a
// lock so previous registrations for this key can be checked. Re-registering a key will result
// in a panic.
func expvarStatMap(addr string) *expvar.Map {
	expvarMu.Lock()
	defer expvarMu.Unlock()

	key := strings.Join([]string{"httpd", addr}, ":")

	// Add expvar for this service.
	var m expvar.Var
	if m = expvar.Get(key); m == nil {
		m = expvar.NewMap(key)
	}
	return m.(*expvar.Map)
}

var expvarMu sync.Mutex
