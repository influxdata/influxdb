package http

import (
	"context"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/influxdata/influxdb/logger"
	"go.uber.org/zap"
)

// DefaultShutdownTimeout is the default timeout for shutting down the http server.
const DefaultShutdownTimeout = 20 * time.Second

// Server is an abstraction around the http.Server that handles a server process.
// It manages the full lifecycle of a server by serving a handler on a socket.
// If signals have been registered, it will attempt to terminate the server using
// Shutdown if a signal is received and will force a shutdown if a second signal
// is received.
type Server struct {
	ShutdownTimeout time.Duration

	srv     *http.Server
	signals map[os.Signal]struct{}
	logger  *zap.Logger
	wg      sync.WaitGroup
}

// NewServer returns a new server struct that can be used.
func NewServer(handler http.Handler, logger *zap.Logger) *Server {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Server{
		ShutdownTimeout: DefaultShutdownTimeout,
		srv: &http.Server{
			Handler:  handler,
			ErrorLog: zap.NewStdLog(logger),
		},
		logger: logger,
	}
}

// Serve will run the server using the listener to accept connections.
func (s *Server) Serve(listener net.Listener) error {
	// When we return, wait for all pending goroutines to finish.
	defer s.wg.Wait()

	signalCh, cancel := s.notifyOnSignals()
	defer cancel()

	errCh := s.serve(listener)
	select {
	case err := <-errCh:
		// The server has failed and reported an error.
		return err
	case <-signalCh:
		// We have received an interrupt. Signal the shutdown process.
		return s.shutdown(signalCh)
	}
}

func (s *Server) serve(listener net.Listener) <-chan error {
	s.wg.Add(1)
	errCh := make(chan error, 1)
	go func() {
		defer s.wg.Done()
		if err := s.srv.Serve(listener); err != nil {
			errCh <- err
		}
		close(errCh)
	}()
	return errCh
}

func (s *Server) shutdown(signalCh <-chan os.Signal) error {
	s.logger.Info("Shutting down server", logger.DurationLiteral("timeout", s.ShutdownTimeout))

	// The shutdown needs to succeed in 20 seconds or less.
	ctx, cancel := context.WithTimeout(context.Background(), s.ShutdownTimeout)
	defer cancel()

	// Wait for another signal to cancel the shutdown.
	done := make(chan struct{})
	defer close(done)

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		select {
		case <-signalCh:
			s.logger.Info("Initializing hard shutdown")
			cancel()
		case <-done:
		}
	}()
	return s.srv.Shutdown(ctx)
}

// ListenForSignals registers the the server to listen for the given signals
// to shutdown the server. The signals are not captured until Serve is called.
func (s *Server) ListenForSignals(signals ...os.Signal) {
	if s.signals == nil {
		s.signals = make(map[os.Signal]struct{})
	}

	for _, sig := range signals {
		s.signals[sig] = struct{}{}
	}
}

func (s *Server) notifyOnSignals() (_ <-chan os.Signal, cancel func()) {
	if len(s.signals) == 0 {
		return nil, func() {}
	}

	// Retrieve which signals we want to be notified on.
	signals := make([]os.Signal, 0, len(s.signals))
	for sig := range s.signals {
		signals = append(signals, sig)
	}

	// Create the signal channel and mark ourselves to be notified
	// of signals. Allow up to two signals for each signal type we catch.
	signalCh := make(chan os.Signal, len(signals)*2)
	signal.Notify(signalCh, signals...)
	return signalCh, func() { signal.Stop(signalCh) }
}

// ListenAndServe is a convenience method for opening a listener using the address
// and then serving the handler on that address. This method sets up the typical
// signal handlers.
func ListenAndServe(addr string, handler http.Handler, logger *zap.Logger) error {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	server := NewServer(handler, logger)
	server.ListenForSignals(os.Interrupt, syscall.SIGTERM)
	return server.Serve(l)
}
