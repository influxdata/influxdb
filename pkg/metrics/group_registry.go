package metrics

import (
	"fmt"
	"sort"
)

// The groupRegistry type represents a set of metrics that are measured together.
type groupRegistry struct {
	desc        *groupDesc
	descriptors []*desc
	group       Group
}

func (g *groupRegistry) register(desc *desc) error {
	p := sort.Search(len(g.descriptors), func(i int) bool {
		return g.descriptors[i].Name == desc.Name
	})

	if p != len(g.descriptors) {
		return fmt.Errorf("metric name '%s' already in use", desc.Name)
	}

	g.descriptors = append(g.descriptors, desc)
	sort.Slice(g.descriptors, func(i, j int) bool {
		return g.descriptors[i].Name < g.descriptors[j].Name
	})

	return nil
}

func (g *groupRegistry) mustRegister(desc *desc) {
	if err := g.register(desc); err != nil {
		panic(err.Error())
	}
}

// MustRegisterCounter registers a new counter metric using the provided descriptor.
// If the metric name is not unique, MustRegisterCounter will panic.
//
// MustRegisterCounter is not safe to call from multiple goroutines.
func (g *groupRegistry) mustRegisterCounter(desc *desc) ID {
	desc.mt = counterMetricType
	g.mustRegister(desc)

	desc.id = newID(len(g.group.counters), g.desc.id)
	g.group.counters = append(g.group.counters, Counter{desc: desc})

	return desc.id
}

// MustRegisterTimer registers a new timer metric using the provided descriptor.
// If the metric name is not unique, MustRegisterTimer will panic.
//
// MustRegisterTimer is not safe to call from multiple goroutines.
func (g *groupRegistry) mustRegisterTimer(desc *desc) ID {
	desc.mt = timerMetricType
	g.mustRegister(desc)

	desc.id = newID(len(g.group.timers), g.desc.id)
	g.group.timers = append(g.group.timers, Timer{desc: desc})

	return desc.id
}

// newCollector returns a Collector with a copy of all the registered counters.
//
// newCollector is safe to call from multiple goroutines.
func (g *groupRegistry) newGroup() *Group {
	c := &Group{
		g:        g,
		counters: make([]Counter, len(g.group.counters)),
		timers:   make([]Timer, len(g.group.timers)),
	}
	copy(c.counters, g.group.counters)
	copy(c.timers, g.group.timers)

	return c
}
