// Package services provides an interface and methods to manage data shared
// between services as well as mechanisms for synchronizing/coordinating
// services.
package services

import (
	"fmt"
	"sync"
)

// A Registry is an interface that describes a type that can encode service
// information.
type Registrar interface {
	WaitFor(string) <-chan struct{} // Returns a channel that blocks until a service is registered.
	IsRunning(string) bool          // Indicates if a service is running by name
	Register(string) error          // Register a service as running
	Unregister(string) error        // Remove a running service
}

// NewRegistry returns an implementation of the Registrar interface.
func NewRegistry() *Registry {
	return &Registry{
		s:  make(map[string]struct{}),
		ch: make(map[string]chan struct{}),
	}
}

// Implementation of the Registrar interface.  There are no exported fields.
type Registry struct {
	mu sync.RWMutex             // rw mutex used to protect our s map
	s  map[string]struct{}      // map storing service names
	ch map[string]chan struct{} // map storing service wait channels
}

// WaitFor takes a string representing a service name and returns a channel
// that unblocks when a service of that name is registered.
//
// This method allows services to block until dependent services are started.
func (l *Registry) WaitFor(s string) <-chan struct{} {
	l.mu.Lock()
	defer l.mu.Unlock()

	if _, ok := l.s[s]; ok {
		// our service is registered.  Return a closed channel to prevent caller
		// from blocking
		rc := make(chan struct{})
		close(rc)
		return rc
	}

	ch, ok := l.ch[s]
	if !ok {
		ch = make(chan struct{})
		l.ch[s] = ch
	}
	return ch
}

// IsRunning returns true if a named service has been registered.
func (l *Registry) IsRunning(s string) bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	_, ok := l.s[s]
	return ok
}

// Register sets a named service as registered.
func (l *Registry) Register(s string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.s[s]; ok {
		return fmt.Errorf("%s already registered", s)
	}
	l.s[s] = struct{}{}

	if ch, ok := l.ch[s]; ok {
		close(ch)
		delete(l.ch, s)
	}
	return nil
}

// Removes a named service from the registery.
func (l *Registry) Unregister(s string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.s[s]; !ok {
		return fmt.Errorf("%s not registered", s)
	}
	delete(l.s, s)
	return nil
}
