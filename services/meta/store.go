package meta

import "sync"

type store struct {
	mu           sync.RWMutex
	index        int
	closing      chan struct{}
	cache        []byte
	cacheChanged chan struct{}
}

func newStore(c *Config) *store {
	s := store{
		index:        1,
		closing:      make(chan struct{}),
		cache:        []byte("hello"),
		cacheChanged: make(chan struct{}),
	}
	return &s
}

func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	close(s.closing)
	return nil
}

func (s *store) SetCache(b []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cache = b
	s.index++
	close(s.cacheChanged)
	s.cacheChanged = make(chan struct{})
}

func (s *store) Snapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.cache, nil
}

// AfterIndex returns a channel that will be closed to signal
// the caller when an updated snapshot is available.
func (s *store) AfterIndex(index int) <-chan struct{} {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if index < s.index {
		// Client needs update so return a closed channel.
		ch := make(chan struct{})
		close(ch)
		return ch
	}

	return s.cacheChanged
}
