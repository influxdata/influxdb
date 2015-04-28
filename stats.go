package influxdb

import (
	"fmt"
	"sort"
	"sync"
)

// Int representes a 64-bit signed integer which can be updated atomically.
type Int struct {
	mu sync.RWMutex
	i  int64
}

// NewInt returns a new Int
func NewInt(v int64) *Int {
	return &Int{i: v}
}

// Add atomically adds the given delta to the Int.
func (i *Int) Add(delta int64) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.i += delta

}

// Stats represents a collection of metrics, as key-value pairs.
type Stats struct {
	name string
	m    map[string]*Int
	mu   sync.RWMutex
}

// NewStats returns a Stats object with the given name.
func NewStats(name string) *Stats {
	return &Stats{
		name: name,
		m:    make(map[string]*Int),
	}
}

// Add adds delta to the stat indiciated by key.
func (s *Stats) Add(key string, delta int64) {
	s.mu.RLock()
	i, ok := s.m[key]
	s.mu.RUnlock()
	if !ok {
		// check again under the write lock
		s.mu.Lock()
		i, ok = s.m[key]
		if !ok {
			i = new(Int)
			s.m[key] = i
		}
		s.mu.Unlock()
	}

	i.Add(delta)
}

// Inc simply increments the given key by 1.
func (s *Stats) Inc(key string) {
	s.Add(key, 1)
}

// Get returns a value for a given key.
func (s *Stats) Get(key string) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.m[key].i
}

// Set sets a value for the given key.
func (s *Stats) Set(key string, v int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[key] = NewInt(v)
}

// Name returns the name of the Stats object.
func (s *Stats) Name() string {
	return s.name
}

// Walk calls f for each entry in the stats. The stats are locked
// during the walk but existing entries may be concurrently updated.
func (s *Stats) Walk(f func(string, int64)) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for k, v := range s.m {
		v.mu.RLock()
		f(k, v.i)
		v.mu.RUnlock()
	}
}

// Diff returns the difference between two sets of stats. The result is undefined
// if the two Stats objects do not contain the same keys.
func (s *Stats) Diff(other *Stats) *Stats {
	diff := NewStats(s.name)
	s.Walk(func(k string, v int64) {
		diff.Set(k, v-other.Get(k))
	})
	return diff
}

// Snapshot returns a copy of the stats object. Addition and removal of stats keys
// is blocked during the created of the snapshot, but existing entries may be
// concurrently updated.
func (s *Stats) Snapshot() *Stats {
	snap := NewStats(s.name)
	s.Walk(func(k string, v int64) {
		snap.Set(k, s.m[k].i)
	})
	return snap
}

func (s *Stats) String() string {
	var out string
	stat := s.Snapshot()
	var keys []string
	for k, _ := range stat.m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out += `{"` + stat.name + `":[`
	var j int
	for _, k := range keys {
		v := stat.m[k].i
		out += `{"` + k + `":` + fmt.Sprintf("%d", v) + `}`
		j++
		if j != len(keys) {
			out += `,`
		}
	}
	out += `]}`
	return out
}
