package metrics

// The Group type represents an instance of a set of measurements that are used for
// instrumenting a specific request.
type Group struct {
	g        *groupRegistry
	counters []Counter
	timers   []Timer
}

// Name returns the name of the group.
func (g *Group) Name() string { return g.g.desc.Name }

// GetCounter returns the counter identified by the id that was returned
// by MustRegisterCounter for the same group.
// Using an id from a different group will result in undefined behavior.
func (g *Group) GetCounter(id ID) *Counter { return &g.counters[id.id()] }

// GetTimer returns the timer identified by the id that was returned
// by MustRegisterTimer for the same group.
// Using an id from a different group will result in undefined behavior.
func (g *Group) GetTimer(id ID) *Timer { return &g.timers[id.id()] }

// The Metric type defines a Name
type Metric interface {
	Name() string
}

// ForEach calls fn for all measurements of the group.
func (g *Group) ForEach(fn func(v Metric)) {
	for i := range g.counters {
		fn(&g.counters[i])
	}
	for i := range g.timers {
		fn(&g.timers[i])
	}
}
