package meta

import (
	"errors"
	"sync"
)

type store struct {
	index        int
	mu           sync.RWMutex
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

func (s *store) Snapshot() (int, []byte) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.index, s.cache
}

func (s *store) WaitForDataChanged(closing chan struct{}) error {
	s.mu.RLock()
	ch := s.cacheChanged
	s.mu.RUnlock()

	for {
		select {
		case <-ch:
			return nil
		case <-closing:
			return errors.New("client closed")
		case <-s.closing:
			return errors.New("store closed")
		}
	}
}
