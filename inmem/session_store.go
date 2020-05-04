package inmem

import (
	"errors"
	"sync"
	"time"
)

type SessionStore struct {
	data map[string]string

	timers map[string]*time.Timer

	mu sync.RWMutex
}

func NewSessionStore() *SessionStore {
	return &SessionStore{
		data:   map[string]string{},
		timers: map[string]*time.Timer{},
	}
}

func (s *SessionStore) Set(key, val string, expireAt time.Time) error {
	if !expireAt.IsZero() && expireAt.Before(time.Now()) {
		// key is already expired. no problem
		return nil
	}

	s.mu.Lock()
	s.data[key] = val
	s.mu.Unlock()

	if !expireAt.IsZero() {
		return s.ExpireAt(key, expireAt)
	}
	return nil
}

func (s *SessionStore) Get(key string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.data[key], nil
}

func (s *SessionStore) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	timer := s.timers[key]
	if timer != nil {
		timer.Stop()
	}

	delete(s.data, key)
	delete(s.timers, key)
	return nil
}

func (s *SessionStore) ExpireAt(key string, expireAt time.Time) error {
	s.mu.Lock()

	existingTimer, ok := s.timers[key]
	if ok {
		if !existingTimer.Stop() {
			return errors.New("session has expired")
		}

	}

	duration := time.Until(expireAt)
	if duration <= 0 {
		s.mu.Unlock()
		s.Delete(key)
		return nil
	}
	s.timers[key] = time.AfterFunc(time.Until(expireAt), s.timerExpireFunc(key))
	s.mu.Unlock()
	return nil
}

func (s *SessionStore) timerExpireFunc(key string) func() {
	return func() {
		s.Delete(key)
	}
}
