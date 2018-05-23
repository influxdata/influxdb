package http

import (
	"context"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"time"

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

	srv      *http.Server
	signalCh chan os.Signal
	logger   *zap.Logger
	wg       sync.WaitGroup
}

// NewServer returns a new server struct that can be used.
func NewServer(handler http.Handler, logger *zap.Logger) *Server {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Server{
		ShutdownTimeout: DefaultShutdownTimeout,
		srv: &http.Server{
			Handler: handler,
		},
		// TODO(jsternberg): Use the logger to report when we are
		// shutting down.
		logger: logger,
	}
}

// Serve will run the server using the listener to accept connections.
func (s *Server) Serve(listener net.Listener) error {
	// When we return, wait for all pending goroutines to finish.
	defer s.wg.Wait()

	errCh := s.serve(listener)
	select {
	case err := <-errCh:
		// The server has failed and reported an error.
		return err
	case <-s.signalCh:
		// We have received an interrupt. Signal the shutdown process.
		return s.shutdown()
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

func (s *Server) shutdown() error {
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
		case <-s.signalCh:
			cancel()
		case <-done:
		}
	}()
	return s.srv.Shutdown(ctx)
}

// ListenForSignals registers the the server to listen for the given signals
// to shutdown the server.
func (s *Server) ListenForSignals(signals ...os.Signal) {
	if s.signalCh == nil {
		s.signalCh = make(chan os.Signal, 4)
	}
	signal.Notify(s.signalCh, signals...)
}
