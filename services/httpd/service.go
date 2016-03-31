package httpd // import "github.com/influxdata/influxdb/services/httpd"

import (
	"crypto/tls"
	"expvar"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/influxdata/influxdb"
)

// statistics gathered by the httpd package.
const (
	statRequest                      = "req"                // Number of HTTP requests served
	statCQRequest                    = "cqReq"              // Number of CQ-execute requests served
	statQueryRequest                 = "queryReq"           // Number of query requests served
	statWriteRequest                 = "writeReq"           // Number of write requests serverd
	statPingRequest                  = "pingReq"            // Number of ping requests served
	statStatusRequest                = "statusReq"          // Number of status requests served
	statWriteRequestBytesReceived    = "writeReqBytes"      // Sum of all bytes in write requests
	statQueryRequestBytesTransmitted = "queryRespBytes"     // Sum of all bytes returned in query reponses
	statPointsWrittenOK              = "pointsWrittenOK"    // Number of points written OK
	statPointsWrittenFail            = "pointsWrittenFail"  // Number of points that failed to be written
	statAuthFail                     = "authFail"           // Number of authentication failures
	statRequestDuration              = "reqDurationNs"      // Number of (wall-time) nanoseconds spent inside requests
	statQueryRequestDuration         = "queryReqDurationNs" // Number of (wall-time) nanoseconds spent inside query requests
	statWriteRequestDuration         = "writeReqDurationNs" // Number of (wall-time) nanoseconds spent inside write requests
	statRequestsActive               = "reqActive"          // Number of currently active requests
)

// Service manages the listener and handler for an HTTP endpoint.
type Service struct {
	ln    net.Listener
	addr  string
	https bool
	cert  string
	err   chan error

	Handler *Handler

	Logger  *log.Logger
	statMap *expvar.Map
}

// NewService returns a new instance of Service.
func NewService(c Config) *Service {
	// Configure expvar monitoring. It's OK to do this even if the service fails to open and
	// should be done before any data could arrive for the service.
	key := strings.Join([]string{"httpd", c.BindAddress}, ":")
	tags := map[string]string{"bind": c.BindAddress}
	statMap := influxdb.NewStatistics(key, "httpd", tags)

	s := &Service{
		addr:  c.BindAddress,
		https: c.HTTPSEnabled,
		cert:  c.HTTPSCertificate,
		err:   make(chan error),
		Handler: NewHandler(
			c.AuthEnabled,
			c.LogEnabled,
			c.WriteTracing,
			c.MaxRowLimit,
			statMap,
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

	// wait for the listeners to start
	timeout := time.Now().Add(time.Second)
	for {
		if s.ln.Addr() != nil {
			break
		}

		if time.Now().After(timeout) {
			return fmt.Errorf("unable to open without http listener running")
		}
		time.Sleep(10 * time.Millisecond)
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
