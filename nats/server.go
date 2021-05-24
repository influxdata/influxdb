package nats

import (
	"errors"

	"github.com/nats-io/gnatsd/server"
	sserver "github.com/nats-io/nats-streaming-server/server"
	"github.com/nats-io/nats-streaming-server/stores"
	"go.uber.org/zap"
)

const ServerName = "platform"

const (
	// RandomPort is the value for port that, when supplied, will cause the
	// server to listen on a randomly-chosen available port. The resolved port
	// will be reassigned to the Port field of server.Options.
	RandomPort = server.RANDOM_PORT
)

var ErrNoNatsConnection = errors.New("nats connection has not been established. Call Open() first")

// Server wraps a connection to a NATS streaming server
type Server struct {
	serverOpts *server.Options
	Server     *sserver.StanServer
	logger     server.Logger
}

// Open starts a NATS streaming server
func (s *Server) Open() error {
	// Streaming options
	opts := sserver.GetDefaultOptions()
	opts.StoreType = stores.TypeMemory
	opts.ID = ServerName
	opts.CustomLogger = s.logger

	server, err := sserver.RunServerWithOpts(opts, s.serverOpts)
	if err != nil {
		return err
	}

	s.Server = server

	return nil
}

// Close stops the embedded NATS server.
func (s *Server) Close() {
	s.Server.Shutdown()
}

// NewDefaultServerOptions returns the default NATS server options, allowing the
// caller to  override specific fields.
func NewDefaultServerOptions() server.Options {
	return sserver.DefaultNatsServerOptions
}

// NewServer creates a new streaming server with the provided server options.
func NewServer(opts *server.Options, log *zap.Logger) *Server {
	if opts == nil {
		o := NewDefaultServerOptions()
		opts = &o
	}
	return &Server{serverOpts: opts, logger: &zapLoggerAdapter{log}}
}
