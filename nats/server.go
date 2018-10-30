package nats

import (
	"errors"

	stand "github.com/nats-io/nats-streaming-server/server"
	"github.com/nats-io/nats-streaming-server/stores"
)

const ServerName = "platform"

var ErrNoNatsConnection = errors.New("Nats connection has not been established. Call Open() first.")

// Server wraps a connection to a NATS streaming server
type Server struct {
	Server *stand.StanServer
	config Config
}

// Open starts a NATS streaming server
func (s *Server) Open() error {
	opts := stand.GetDefaultOptions()
	opts.StoreType = stores.TypeFile
	opts.ID = ServerName
	opts.FilestoreDir = s.config.FilestoreDir
	server, err := stand.RunServerWithOpts(opts, nil)
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

// Config is the configuration for the NATS streaming server
type Config struct {
	// The directory where nats persists message information
	FilestoreDir string
}

// NewServer creates and returns a new server struct from the provided config
func NewServer(c Config) *Server {
	return &Server{
		config: c,
	}
}
