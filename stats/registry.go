package stats

import (
	"sync"
)

var (
	root *registry
	// Root is a reference the Registry singleton.
	Root Registry
)

// Ensure that container is always defined and contains a "statistics" map.
func init() {
	root = newRegistry()
	Root = root
}

// A type used to allow callbacks to be deregistered
type listener struct {
	callback func(registration)
	closer   func()
}

// A type used to represent a registry of all Statistics objects
type registry struct {
	mu            sync.RWMutex
	listeners     []*listener
	registrations map[string]registration
}

// Create a new builder that retains a reference to the registry.
func (r *registry) NewBuilder(k string, n string, tags map[string]string) Builder {
	return newBuilder(k, n, tags, r)
}

func newRegistry() *registry {
	return &registry{
		listeners:     make([]*listener, 0),
		registrations: map[string]registration{},
	}
}

// Open a new view over the contents of the registry.
func (r *registry) Open() View {
	return newView(r)
}

// Cleans the registry to remove statistics that have been closed.
func (r *registry) clean() {
	r.mu.Lock()
	defer r.mu.Unlock()

	toclean := []string{}

	for k, g := range r.registrations {
		if g.refs() == 0 {
			toclean = append(toclean, k)
		}
	}

	for _, k := range toclean {
		delete(r.registrations, k)
	}
}

// registry is used by newly opened Statistics objects to notify onOpen
// listeners that a new Statistics object has been registered.
func (r *registry) register(g registration) {
	// clone the existing list of listeners
	r.mu.Lock()
	clone := make([]*listener, len(r.listeners))
	copy(clone, r.listeners)
	r.registrations[g.Key()] = g
	r.mu.Unlock()

	// call the each of the cloned listeners without holding any lock
	for _, l := range clone {
		l.callback(g)
	}
	return
}

// onOpen registers a new OnOpen listener. The listener will receive notifications for
// all open Statistics currently in the Registry and for any objects that are
// subsequently added.
func (r *registry) onOpen(lf func(o registration)) func() {

	existing := []registration{}

	// add a new listener while holding the write lock
	r.mu.Lock()
	l := &listener{
		callback: lf,
	}
	l.closer = func() {
		r.mu.Lock()
		defer r.mu.Unlock()

		for i, e := range r.listeners {
			if e == l {
				r.listeners = append(r.listeners[:i], r.listeners[i+1:]...)
				return
			}
		}
	}

	for _, g := range r.registrations {
		existing = append(existing, g)
	}

	r.listeners = append(r.listeners, l)
	r.mu.Unlock()

	// Call the listener on objects that were already in the map before we added a listener.
	for _, g := range existing {
		if g.isOpen() {
			lf(g)
		}
	}

	// By the time we get here, the listener has received one notification for
	// each Statistics object that was in the map prior to the listener being registered
	// and one notification for each added since. The notifications won't necessarily be received
	// in order of their original delivery to other listeners.

	return l.closer
}
