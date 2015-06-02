package influxdb

import (
	"fmt"
	"sort"
	"sync"
)

// Stats represents a collection of metrics as key-value pairs.
type Stats struct {
	mu     sync.RWMutex
	name   string
	values map[string]int64
}

// NewStats returns a Stats object with the given name.
func NewStats(name string) *Stats {
	return &Stats{
		name:   name,
		values: make(map[string]int64),
	}
}

// Name returns the name of the Stats object.
func (s *Stats) Name() string { return s.name }

// Get returns a value for a given key.
func (s *Stats) Get(key string) int64 {
	s.mu.RLock()
	v := s.values[key]
	s.mu.RUnlock()
	return v
}

// Set sets a value for the given key.
func (s *Stats) Set(key string, v int64) {
	s.mu.Lock()
	s.values[key] = v
	s.mu.Unlock()
}

// Add adds delta to the stat indiciated by key.
func (s *Stats) Add(key string, delta int64) {
	s.mu.Lock()
	s.values[key] += delta
	s.mu.Unlock()
}

// Inc simply increments the given key by 1.
func (s *Stats) Inc(key string) { s.Add(key, 1) }

// Walk calls f for each entry in the stats.
func (s *Stats) Walk(fn func(string, int64)) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for k, v := range s.values {
		fn(k, v)
	}
}

// Diff returns the difference between two sets of stats.
// The result is undefined if the two Stats objects do not contain the same keys.
func (s *Stats) Diff(other *Stats) *Stats {
	diff := NewStats(s.name)
	s.Walk(func(k string, v int64) {
		diff.Set(k, v-other.Get(k))
	})
	return diff
}

// Clone returns a copy of the stats object.
func (s *Stats) Clone() *Stats {
	other := NewStats(s.name)
	s.Walk(func(k string, v int64) {
		other.Set(k, s.values[k])
	})
	return other
}

func (s *Stats) String() string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var out string
	var keys []string
	for k, _ := range s.values {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out += `{"` + s.name + `":[`

	var j int
	for _, k := range keys {
		out += fmt.Sprintf(`{"%s":%d}`, k, s.values[k])
		j++
		if j != len(keys) {
			out += `,`
		}
	}
	out += `]}`
	return out
}
