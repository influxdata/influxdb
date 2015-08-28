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
	mu sync.Mutex
	registrations map[string]Client
}

// Register registers a client with the given name. It is an error to register a client with
// an already registered key.
func (s *Service) Register(key string, client Client) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.registrations[key]; ok {
		return fmt.Errorf("existing client registered with key %s", key)
	}

	s.registrations[key] = client
	return nil
}

// Deregister deregisters any client previously registered with the existing key.
func (s *Service) Deregister(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.registrations, key)
}