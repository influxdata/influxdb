package udp

import (
	"net"
	"time"
)

type Service struct {
	Server *Server

	addr string
}

func NewService(c Config) *Service {
	server := NewServer(c.Database)
	server.SetBatchSize(c.BatchSize)
	server.SetBatchTimeout(time.Duration(c.BatchTimeout))

	return &Service{
		addr:   c.BindAddress,
		Server: server,
	}
}

func (s *Service) Open() error {
	return s.Server.ListenAndServe(s.addr)
}

func (s *Service) Close() error {
	return s.Server.Close()
}

func (s *Service) Addr() net.Addr {
	return s.Server.Addr()
}
