package cluster

import "net"

type Service struct {
}

func NewService(c Config) *Service {
	return &Service{}
}

func (s *Service) Open() error    { return nil }
func (s *Service) Close() error   { return nil }
func (s *Service) Addr() net.Addr { return nil }
