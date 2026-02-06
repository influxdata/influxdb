// Package httpd implements the HTTP service and REST API for InfluxDB.
package httpd // import "github.com/influxdata/influxdb/services/httpd"

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/tlsconfig"
	"go.uber.org/zap"
)

// statistics gathered by the httpd package.
const (
	statRequest                          = "req"                    // Number of HTTP requests served.
	statQueryRequest                     = "queryReq"               // Number of query requests served.
	statWriteRequest                     = "writeReq"               // Number of write requests served.
	statPingRequest                      = "pingReq"                // Number of ping requests served.
	statStatusRequest                    = "statusReq"              // Number of status requests served.
	statWriteRequestBytesReceived        = "writeReqBytes"          // Sum of all bytes in write requests.
	statQueryRequestBytesTransmitted     = "queryRespBytes"         // Sum of all bytes returned in query responses.
	statPointsWrittenOK                  = "pointsWrittenOK"        // Number of points written OK.
	statValuesWrittenOK                  = "valuesWrittenOK"        // Number of values (fields) written OK.
	statPointsWrittenDropped             = "pointsWrittenDropped"   // Number of points dropped by the storage engine.
	statPointsWrittenFail                = "pointsWrittenFail"      // Number of points that failed to be written.
	statAuthFail                         = "authFail"               // Number of authentication failures.
	statRequestDuration                  = "reqDurationNs"          // Number of (wall-time) nanoseconds spent inside requests.
	statQueryRequestDuration             = "queryReqDurationNs"     // Number of (wall-time) nanoseconds spent inside query requests.
	statWriteRequestDuration             = "writeReqDurationNs"     // Number of (wall-time) nanoseconds spent inside write requests.
	statRequestsActive                   = "reqActive"              // Number of currently active requests.
	statWriteRequestsActive              = "writeReqActive"         // Number of currently active write requests.
	statClientError                      = "clientError"            // Number of HTTP responses due to client error.
	statServerError                      = "serverError"            // Number of HTTP responses due to server error.
	statRecoveredPanics                  = "recoveredPanics"        // Number of panics recovered by HTTP handler.
	statPromWriteRequest                 = "promWriteReq"           // Number of write requests to the prometheus endpoint.
	statPromReadRequest                  = "promReadReq"            // Number of read requests to the prometheus endpoint.
	statFluxQueryRequests                = "fluxQueryReq"           // Number of flux query requests served.
	statFluxQueryRequestDuration         = "fluxQueryReqDurationNs" // Number of (wall-time) nanoseconds spent executing Flux query requests.
	statFluxQueryRequestBytesTransmitted = "fluxQueryRespBytes"     // Sum of all bytes returned in Flux query responses.
	statUserQueryRespBytes               = "userQueryRespBytes"     // Value field for per-user query response bytes.

	// StatUserTagKey is the tag key used to identify users in per-user statistics.
	StatUserTagKey = "user"
	// StatAnonymousUser is the tag value for unauthenticated users in statistics.
	StatAnonymousUser = "(anonymous)"
)

// Service manages the listener and handler for an HTTP endpoint.
type Service struct {
	// Fields above mu are not protected by mu and should be read-only after being
	// set in NewService.

	addr  string
	https bool

	unixSocket      bool
	unixSocketPerm  uint32
	unixSocketGroup int
	bindSocket      string

	// cert is the initial TLS certificate to load if https is true. This should not be
	// exposed to client code because a different certificate might be loaded on a config reload.
	cert string

	// key is the initial TLS key to load if https is true. This should not be
	// exposed to client code because a key certificate might be loaded on a config reload.
	key string

	// insecureCert is true if certificate file permissions should be ignored.
	insecureCert bool

	limit     int
	tlsConfig *tls.Config
	err       chan error

	closeFunc func() error

	// Handler for httpd service. Handler is not protected by mu, and should only be modified
	// before calling Open.
	Handler    *Handler
	httpServer http.Server

	Logger *zap.Logger

	// mu protects the fields that follow.
	mu sync.RWMutex

	ln                 net.Listener
	unixSocketListener net.Listener
	tlsManager         *tlsconfig.TLSConfigManager

	// tcpServerStarted indicates if httpServer is started for ln, which in turn indicates who is
	// responsible for closing ln.
	tcpServerStarted bool

	// unixServerStarted indicates if httpServer is started unixSocketListener, which in turn
	// indicates who is responsible for closing unixSocketListener.
	unixServerStarted bool

	// closed indicates if the Service has been closed. This is used to prevent opening the
	// service after it has been shutdown.
	closed bool
}

// NewService returns a new instance of Service.
func NewService(c Config) *Service {
	handler := NewHandler(c)
	s := &Service{
		addr:           c.BindAddress,
		https:          c.HTTPSEnabled,
		cert:           c.HTTPSCertificate,
		key:            c.HTTPSPrivateKey,
		insecureCert:   c.HTTPSInsecureCertificate,
		limit:          c.MaxConnectionLimit,
		tlsConfig:      c.TLS,
		err:            make(chan error, 2), // There could be two serve calls that fail.
		unixSocket:     c.UnixSocketEnabled,
		unixSocketPerm: uint32(c.UnixSocketPermissions),
		bindSocket:     c.BindSocket,
		Handler:        handler,
		httpServer: http.Server{
			Handler: handler,
		},
		Logger: zap.NewNop(),
	}
	s.closeFunc = sync.OnceValue(s.doClose)
	if s.tlsConfig == nil {
		s.tlsConfig = new(tls.Config)
	}
	if c.UnixSocketGroup != nil {
		s.unixSocketGroup = int(*c.UnixSocketGroup)
	}
	s.Handler.Logger = s.Logger
	return s
}

// Open starts the service.
func (s *Service) Open() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.Logger.Info("Starting HTTP service", zap.Bool("authentication", s.Handler.Config.AuthEnabled))

	if s.closed {
		return fmt.Errorf("attempt to Open http service after Close")
	}

	s.Handler.Open()

	// Open listener.
	tm, err := tlsconfig.NewTLSConfigManager(s.https, s.tlsConfig, s.cert, s.key, false,
		tlsconfig.WithAllowInsecure(s.insecureCert),
		tlsconfig.WithLogger(s.Logger))
	if err != nil {
		return fmt.Errorf("httpd: error creating TLS manager: %w", err)
	}
	s.tlsManager = tm

	if ln, err := s.tlsManager.Listen("tcp", s.addr); err != nil {
		return fmt.Errorf("httpd: error creating listener: %w", err)
	} else {
		s.ln = ln
	}
	s.Logger.Info("Listening on HTTP",
		zap.Stringer("addr", s.ln.Addr()),
		zap.Bool("https", s.https))

	// Open unix socket listener.
	if s.unixSocket {
		if runtime.GOOS == "windows" {
			return fmt.Errorf("unable to use unix socket on windows")
		}
		if err := os.MkdirAll(path.Dir(s.bindSocket), 0777); err != nil {
			return err
		}
		if err := syscall.Unlink(s.bindSocket); err != nil && !os.IsNotExist(err) {
			return err
		}

		listener, err := net.Listen("unix", s.bindSocket)
		if err != nil {
			return err
		}
		if s.unixSocketPerm != 0 {
			if err := os.Chmod(s.bindSocket, os.FileMode(s.unixSocketPerm)); err != nil {
				return err
			}
		}
		if s.unixSocketGroup != 0 {
			if err := os.Chown(s.bindSocket, -1, s.unixSocketGroup); err != nil {
				return err
			}
		}

		s.Logger.Info("Listening on unix socket",
			zap.Stringer("addr", listener.Addr()))
		s.unixSocketListener = listener

		go s.serve(s.unixSocketListener)
		s.unixServerStarted = true
	}

	// Enforce a connection limit if one has been given.
	if s.limit > 0 {
		s.ln = LimitListener(s.ln, s.limit)
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
	go s.serve(s.ln)
	s.tcpServerStarted = true
	return nil
}

// Close shuts down the httpd service, including closing the listeners.
func (s *Service) Close() error {
	return s.closeFunc()
}

// doClose performs the actual work of closing httpd service, including listeners.
// Do not call doClose directly. Use the sync.OnceValue wrapped version created by
// NewService in the closeFunc field.
func (s *Service) doClose() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.Handler.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s.closed = true
	if err := s.httpServer.Shutdown(ctx); err != nil {
		return err
	}

	// Close listeners, but only if the server hasn't started. Once the server starts,
	// s.httpServer takes over management of the listener.
	if !s.tcpServerStarted {
		if s.ln != nil {
			if err := s.ln.Close(); err != nil {
				return err
			}
		}
	}
	if !s.unixServerStarted {
		if s.unixSocketListener != nil {
			if err := s.unixSocketListener.Close(); err != nil {
				return err
			}
		}
	}

	if s.tlsManager != nil {
		// It is safe to call tlsManager.Close multiple times.
		if err := s.tlsManager.Close(); err != nil {
			return err
		}
	}

	return nil
}

// WithLogger sets the logger for the service. WithLogger should only be called before Open.
func (s *Service) WithLogger(log *zap.Logger) {
	s.Logger = log.With(zap.String("service", "httpd"))
	s.Handler.Logger = s.Logger
}

// PrepareReloadConfig checks if c can be applied. If it can be, a function
// that will apply the reloaded configuration is returned. No function is
// returned if no action is required.
func (s *Service) PrepareReloadConfig(c Config) (func() error, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Let the user know that changing the https-enabled setting doesn't work.
	if s.https != c.HTTPSEnabled {
		return nil, fmt.Errorf("httpd: can not change https-enabled on a running server")
	}

	if s.https {
		// Sanity check to make sure we have a tlsManager. It's possible this could happen if a
		// reload signal is sent to the process after NewService but before Open. By returning an
		// error here the reload will fail and no changes will be made.
		if s.tlsManager == nil {
			return nil, errors.New("httpd: no TLS manager available")
		}

		// Make sure the specified certificate will load correctly and return an apply function.
		if apply, err := s.tlsManager.PrepareCertificateLoad(c.HTTPSCertificate, c.HTTPSPrivateKey); err == nil {
			return apply, nil
		} else {
			return nil, fmt.Errorf("httpd: error loading certificate at (%q, %q): %w", c.HTTPSCertificate, c.HTTPSPrivateKey, err)
		}
	}

	return nil, nil
}

// Err returns a channel for fatal errors that occur on the listener.
func (s *Service) Err() <-chan error { return s.err }

// Addr returns the listener's address. Returns nil if listener is closed.
func (s *Service) Addr() net.Addr {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.ln != nil {
		return s.ln.Addr()
	}
	return nil
}

// Statistics returns statistics for periodic monitoring.
func (s *Service) Statistics(tags map[string]string) []models.Statistic {
	return s.Handler.Statistics(models.NewTags(map[string]string{"bind": s.addr}).Merge(tags).Map())
}

// BoundHTTPAddr returns the string version of the address that the HTTP server is listening on.
// This is useful if you start an ephemeral server in test with bind address localhost:0.
func (s *Service) BoundHTTPAddr() string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.ln != nil {
		return s.ln.Addr().String()
	}
	return "N/A"
}

// serve serves the handler from the listener.
func (s *Service) serve(ln net.Listener) {
	// The listener was closed so exit
	// See https://github.com/golang/go/issues/4373
	if err := s.httpServer.Serve(ln); err != nil && !strings.Contains(err.Error(), "closed") {
		s.err <- fmt.Errorf("listener failed: addr=%s, err=%s", ln.Addr(), err)
	}
}
