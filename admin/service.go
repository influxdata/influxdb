package admin

import "log"

type Service struct {
}

func NewService(c *Config) *Service {
	return &Service{}
}

func (s *Service) Open() error {

	if err := cmd.node.openAdminServer(cmd.config.Admin.Port); err != nil {
		log.Fatalf("admin server failed to listen on :%d: %s", cmd.config.Admin.Port, err)
	}
	log.Printf("admin server listening on :%d", cmd.config.Admin.Port)
}

func (s *Service) Close() error { return nil }

type Config struct {
	Enabled bool `toml:"enabled"`
	Port    int  `toml:"port"`
}

func (s *Node) closeAdminServer() error {
	if s.adminServer != nil {
		return s.adminServer.Close()
	}
	return nil
}
