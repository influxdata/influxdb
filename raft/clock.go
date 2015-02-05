package raft

import (
	"time"
)

const (
	// DefaultApplyInterval is the default time between checks to apply commands.
	DefaultApplyInterval = 10 * time.Millisecond

	// DefaultElectionTimeout is the default time before starting an election.
	DefaultElectionTimeout = 500 * time.Millisecond

	// DefaultHeartbeatInterval is the default time to wait between heartbeats.
	DefaultHeartbeatInterval = 150 * time.Millisecond

	// DefaultReconnectTimeout is the default time to wait before reconnecting.
	DefaultReconnectTimeout = 10 * time.Millisecond

	// DefaultWaitInterval is the default time to wait log sync.
	DefaultWaitInterval = 1 * time.Millisecond
)

// Clock implements an interface to the real-time clock.
type Clock struct {
	ApplyInterval     time.Duration
	ElectionTimeout   time.Duration
	HeartbeatInterval time.Duration
	ReconnectTimeout  time.Duration
	WaitInterval      time.Duration
}

// NewClock returns a instance of Clock with defaults set.
func NewClock() *Clock {
	return &Clock{
		ApplyInterval:     DefaultApplyInterval,
		ElectionTimeout:   DefaultElectionTimeout,
		HeartbeatInterval: DefaultHeartbeatInterval,
		ReconnectTimeout:  DefaultReconnectTimeout,
		WaitInterval:      DefaultWaitInterval,
	}
}

// AfterApplyInterval returns a channel that fires after the apply interval.
func (c *Clock) AfterApplyInterval() <-chan chan struct{} { return newClockChan(c.ApplyInterval) }

// AfterElectionTimeout returns a channel that fires after the election timeout.
func (c *Clock) AfterElectionTimeout() <-chan chan struct{} { return newClockChan(c.ElectionTimeout) }

// AfterHeartbeatInterval returns a channel that fires after the heartbeat interval.
func (c *Clock) AfterHeartbeatInterval() <-chan chan struct{} {
	return newClockChan(c.HeartbeatInterval)
}

// AfterReconnectTimeout returns a channel that fires after the reconnection timeout.
func (c *Clock) AfterReconnectTimeout() <-chan chan struct{} { return newClockChan(c.ReconnectTimeout) }

// AfterWaitInterval returns a channel that fires after the wait interval.
func (c *Clock) AfterWaitInterval() <-chan chan struct{} { return newClockChan(c.WaitInterval) }

// HeartbeatTicker returns a Ticker that ticks every heartbeat.
func (c *Clock) HeartbeatTicker() *Ticker {
	t := time.NewTicker(c.HeartbeatInterval)
	return &Ticker{C: t.C, ticker: t}
}

// Now returns the current wall clock time.
func (c *Clock) Now() time.Time { return time.Now() }

// Ticker holds a channel that receives "ticks" at regular intervals.
type Ticker struct {
	C      <-chan time.Time
	ticker *time.Ticker // realtime impl, if set
}

// Stop turns off the ticker.
func (t *Ticker) Stop() {
	if t.ticker != nil {
		t.ticker.Stop()
	}
}

// newClockChan returns a channel that sends a channel after a given duration.
// The channel being sent, over the channel that is returned, can be used to
// notify the sender when an action is done.
func newClockChan(d time.Duration) <-chan chan struct{} {
	ch := make(chan chan struct{})
	go func() {
		time.Sleep(d)
		ch <- make(chan struct{})
	}()
	return ch
}
