package raft

import (
	"math/rand"
	"time"
)

const (
	// DefaultApplyInterval is the default time between checks to apply commands.
	DefaultApplyInterval = 10 * time.Millisecond

	// DefaultElectionTimeout is the default time before starting an election.
	DefaultElectionTimeout = 1 * time.Second

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

// AfterApplyInterval returns a channel that fires after the apply interval.
func (c *Clock) AfterApplyInterval() <-chan chan struct{} { return newClockChan(c.ApplyInterval) }

// AfterElectionTimeout returns a channel that fires after a duration that is
// between the election timeout and double the election timeout.
func (c *Clock) AfterElectionTimeout() <-chan chan struct{} {
	rand.Seed(time.Now().UnixNano())
	d := c.ElectionTimeout + time.Duration(rand.Intn(int(c.ElectionTimeout)))
	return newClockChan(d)
}

// AfterHeartbeatInterval returns a channel that fires after the heartbeat interval.
func (c *Clock) AfterHeartbeatInterval() <-chan chan struct{} {
	return newClockChan(c.HeartbeatInterval)
}

// AfterReconnectTimeout returns a channel that fires after the reconnection timeout.
func (c *Clock) AfterReconnectTimeout() <-chan chan struct{} { return newClockChan(c.ReconnectTimeout) }

// Now returns the current wall clock time.
func (c *Clock) Now() time.Time { return time.Now() }

// newClockChan returns a channel that sends a channel after a given duration.
// The channel being sent, over the channel that is returned, can be used to
// notify the sender when an action is done.
func newClockChan(d time.Duration) <-chan chan struct{} {
	ch := make(chan chan struct{}, 1)
	go func() { time.Sleep(d); ch <- make(chan struct{}) }()
	return ch
}
