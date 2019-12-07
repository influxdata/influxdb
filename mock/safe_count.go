package mock

import (
	"sync"
)

// SafeCount provides a safe counter, useful for call counts to maintain
// thread safety. Removes burden of having to introduce serialization when
// concurrency is brought in.
type SafeCount struct {
	mu sync.Mutex
	i  int
}

// IncrFn increments the safe counter by 1.
func (s *SafeCount) IncrFn() func() {
	s.mu.Lock()
	return func() {
		s.i++
		s.mu.Unlock()
	}
}

// Count returns the current count.
func (s *SafeCount) Count() int {
	return s.i
}

// Reset will reset the count to 0.
func (s *SafeCount) Reset() {
	s.mu.Lock()
	{
		s.i = 0
	}
	s.mu.Unlock()
}
