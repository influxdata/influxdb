package stats

import (
	"sync"
)

// A view represents a stable view of registrations. Registrations that are closed
// between two calls to the view's Do method do not disappear from the view
// until after the second call is complete.
type view struct {
	mu            sync.RWMutex
	registry      registryClient
	registrations map[string]registration
	closer        func()
}

// newView creates a new view and registers an observer with the registry.
func newView(r registryClient) View {
	v := &view{
		registry:      r,
		registrations: make(map[string]registration),
	}
	v.closer = r.onOpen(v.onOpen)
	return v
}

// Close release references to all registrations in the view, then pings the registry
// to give it a chance to clean itself.
func (v *view) Close() {
	tmp := v.registrations
	v.closer()

	v.mu.Lock()
	v.registrations = make(map[string]registration)
	v.mu.Unlock()

	count := 0
	for _, g := range tmp {
		if g.stopObserving() == 0 {
			count++
		}
	}
	if count > 0 {
		v.registry.clean()
	}
}

//
// Iterates over the registry, holding a read lock.
//
// The iteration skips over closed statistics.
//
// If any closed statistics are detected during
// the operation, then the "statistics" map is
// cleansed by creating a new map and copying
// only those statistics that are still open.
//
func (v *view) Do(f func(s Statistics)) View {

	forgotten := map[string]registration{}

	// iterate over the view while hold the views read lock
	v.mu.RLock()
	for k, g := range v.registrations {
		f(g)
		if !g.isOpen() {
			forgotten[k] = g
		}
	}
	v.mu.RUnlock()

	// update the view to remove closed registrations from the map
	v.mu.Lock()
	for k, g1 := range forgotten {
		if g2, ok := v.registrations[k]; ok && (g1 == g2) {
			delete(v.registrations, k)
		}
	}
	v.mu.Unlock()

	// remove the references to the closed registrations
	count := 0
	for _, g := range forgotten {
		if g.stopObserving() == 0 {
			count++
		}
	}

	// ping the registry give it a chance to clean itself
	if count > 0 {
		v.registry.clean()
	}
	return v
}

// onOpen is called once for every registration in the registry at the time the view
// is opened and once for each registration created after that time until the view
// is closed.
func (v *view) onOpen(g registration) {
	g.observe() // outside of a lock

	v.mu.Lock()
	defer v.mu.Unlock()

	v.registrations[g.Key()] = g
}
