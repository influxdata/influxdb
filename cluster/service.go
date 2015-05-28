package cluster

type Service struct {
}

func NewService(c *Config) *Service {
	return &Service{}
}

func (s *Service) Open() error  { return nil }
func (s *Service) Close() error { return nil }

type Config struct{}
