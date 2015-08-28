package monitor

import (
	"expvar"
	"fmt"
	"sync"
)

// Client is the interface modules must implement if they wish to register with monitor.
type Client interface {
	Statistics() (expvar.Map, error)
	Diagnostics() (map[string]interface{}, error)
}

type Service struct {
	mu            sync.Mutex
	registrations map[string]Client
}

// Register registers a client with the given name. It is an error to register a client with
// an already registered key.
func (s *Service) Register(name string, tags map[string]string, client Client) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.registrations[name]; ok {
		return fmt.Errorf("existing client registered with name %s", name)
	}

	s.registrations[name] = client
	return nil
}

// Deregister deregisters any client previously registered with the existing name.
func (s *Service) Deregister(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.registrations, name)
}
