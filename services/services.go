package services

import (
	"fmt"
	"sync"
)

const (
	RegistryContextKey = "registry" // a value to use as the context key for a registry
)

type Registry interface {
	IsRunning(string) bool   // indicates if a service is running by name
	Register(string) error   // register a service as running
	Unregister(string) error // remove a running service
}

func NewRegistry() Registry {
	return &localRegistry{
		s: make(map[string]struct{}),
	}
}

type localRegistry struct {
	sync.RWMutex                     // rw mutex used to protect our s map
	s            map[string]struct{} // map storing service names
}

func (l *localRegistry) IsRunning(s string) bool {
	l.RLock()
	defer l.RUnlock()
	_, ok := l.s[s]
	return ok
}

func (l *localRegistry) Register(s string) error {
	l.Lock()
	defer l.Unlock()
	if _, ok := l.s[s]; ok {
		return fmt.Errorf("%s already registered", s)
	}
	l.s[s] = struct{}{}
	return nil
}

func (l *localRegistry) Unregister(s string) error {
	l.Lock()
	defer l.Unlock()
	if _, ok := l.s[s]; !ok {
		return fmt.Errorf("%s not registered", s)
	}
	delete(l.s, s)
	return nil
}
