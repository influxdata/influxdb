package raft

import "time"

// MockTime represents an alternative implementation of the time package that
// can be controlled programmatically.
type MockTime struct {
	now    time.Time
	adding bool // tracks whether the mock is currently adding time.
}

// Now returns the current mock time.
func (m *MockTime) Now() time.Time {
	return m.now
}

// SetNow sets the current mock time.
func (m *MockTime) SetNow(t time.Time) {
	if t.Before(m.now) {
		panic("cannot set now to before current time")
	}
	m.now = t
}

// Add increments the current time by the given amount of time.
func (m *MockTime) Add(d time.Duration) {
	// Don't allow us to add time while time is already being added.
	if m.adding {
		panic("cannot add mock time while already adding time")
	}
	m.adding = true

	// TODO(benbjohnson): Iterate over timers and execute them in order.

	// Move the current time forward.
	m.now = m.now.Add(d)

	m.adding = false
}

// MockTime sets a mock time implementation on the log and returns the mock.
func (l *Log) SetTime(t *MockTime) {
	l.time = t
}
