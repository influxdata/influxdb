package raft

import (
	"math/rand"
	"time"
)

const (
	// DefaultApplyInterval is the default time between checks to apply commands.
	DefaultApplyInterval = 10 * time.Millisecond

	// DefaultElectionTimeout is the default time before starting an election.
	DefaultElectionTimeout = 5 * time.Second

	// DefaultHeartbeatInterval is the default time to wait between heartbeats.
	DefaultHeartbeatInterval = 100 * time.Millisecond

	// DefaultReconnectTimeout is the default time to wait before reconnecting.
	DefaultReconnectTimeout = 10 * time.Millisecond
)

// Clock implements an interface to the real-time clock.
type Clock struct {
	ApplyInterval     time.Duration
	ElectionTimeout   time.Duration
	HeartbeatInterval time.Duration
	ReconnectTimeout  time.Duration
}

// NewClock returns a instance of Clock with defaults set.
func NewClock() *Clock {
	return &Clock{
		ApplyInterval:     DefaultApplyInterval,
		ElectionTimeout:   DefaultElectionTimeout,
		HeartbeatInterval: DefaultHeartbeatInterval,
		ReconnectTimeout:  DefaultReconnectTimeout,
	}
}

// ApplyTimer returns a timer that fires after the apply interval.
func (c *Clock) ApplyTimer() Timer { return NewTimer(c.ApplyInterval) }

// AfterElectionTimeout returns a channel that fires after a duration that is
// between the election timeout and double the election timeout.
func (c *Clock) ElectionTimer() Timer {
	rand.Seed(time.Now().UnixNano())
	d := c.ElectionTimeout + time.Duration(rand.Intn(int(c.ElectionTimeout)))
	return NewTimer(d)
}

// HeartbeatTimer returns a timer that fires after the heartbeat interval.
func (c *Clock) HeartbeatTimer() Timer {
	return NewTimer(c.HeartbeatInterval)
}

// ReconnectTimer returns a timer that fires after the reconnection timeout.
func (c *Clock) ReconnectTimer() Timer { return NewTimer(c.ReconnectTimeout) }

// Now returns the current wall clock time.
func (c *Clock) Now() time.Time { return time.Now() }

type Timer interface {
	C() <-chan chan struct{}
	Stop()
}

// NewTimer returns a channel that sends a channel after a given duration.
// The channel being sent, over the channel that is returned, can be used to
// notify the sender when an action is done.
func NewTimer(d time.Duration) Timer {
	t := &timer{
		Timer:   time.NewTimer(d),
		closing: make(chan struct{}, 0),
		c:       make(chan chan struct{}, 1),
	}
	go t.run()
	return t
}

type timer struct {
	*time.Timer
	closing chan struct{}
	c       chan chan struct{}
}

func (t *timer) C() <-chan chan struct{} {
	return t.c
}

func (t *timer) Stop() {
	t.Timer.Stop()
	close(t.closing)
}

func (t *timer) run() {
	select {
	case <-t.closing:
	case <-t.Timer.C:
		t.c <- make(chan struct{})
	}
}
