package nats

import (
	"errors"

	stand "github.com/nats-io/nats-streaming-server/server"
	"github.com/nats-io/nats-streaming-server/stores"
)

const ServerName = "platform"

var ErrNoNatsConnection = errors.New("nats connection has not been established. Call Open() first")

// Server wraps a connection to a NATS streaming server
type Server struct {
	Server *stand.StanServer
}

// Open starts a NATS streaming server
func (s *Server) Open() error {
	opts := stand.GetDefaultOptions()
	opts.StoreType = stores.TypeMemory
	opts.ID = ServerName
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

// NewServer creates and returns a new server struct from the provided config
func NewServer() *Server {
	return &Server{}
}
