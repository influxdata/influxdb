package raft_test

import (
	"flag"
	"time"
)

var (
	goschedTimeout = flag.Duration("gosched", 100*time.Millisecond, "gosched() delay")
)

// DefaultTime represents the time that the test clock is initialized to.
// Defaults to midnight on Jan 1, 2000 UTC
var DefaultTime = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

// Clock represents a testable clock.
type Clock struct {
	now           time.Time
	applyChan     chan chan struct{}
	electionChan  chan chan struct{}
	heartbeatChan chan chan struct{}
	reconnectChan chan chan struct{}

	NowFunc                    func() time.Time
	AfterApplyIntervalFunc     func() <-chan chan struct{}
	AfterElectionTimeoutFunc   func() <-chan chan struct{}
	AfterHeartbeatIntervalFunc func() <-chan chan struct{}
	AfterReconnectTimeoutFunc  func() <-chan chan struct{}
}

// NewClock returns an instance of Clock with default.
func NewClock() *Clock {
	c := &Clock{
		now:           DefaultTime,
		applyChan:     make(chan chan struct{}, 0),
		electionChan:  make(chan chan struct{}, 0),
		heartbeatChan: make(chan chan struct{}, 0),
		reconnectChan: make(chan chan struct{}, 0),
	}

	// Set default functions.
	c.NowFunc = func() time.Time { return c.now }
	c.AfterApplyIntervalFunc = func() <-chan chan struct{} { return c.applyChan }
	c.AfterElectionTimeoutFunc = func() <-chan chan struct{} { return c.electionChan }
	c.AfterHeartbeatIntervalFunc = func() <-chan chan struct{} { return c.heartbeatChan }
	c.AfterReconnectTimeoutFunc = func() <-chan chan struct{} { return c.reconnectChan }
	return c
}

func (c *Clock) apply() {
	ch := make(chan struct{}, 0)
	c.applyChan <- ch
	<-ch
}

func (c *Clock) election() {
	ch := make(chan struct{}, 0)
	c.electionChan <- ch
	<-ch
}

func (c *Clock) heartbeat() {
	ch := make(chan struct{}, 0)
	c.heartbeatChan <- ch
	<-ch
}

func (c *Clock) reconnect() {
	ch := make(chan struct{}, 0)
	c.reconnectChan <- ch
	<-ch
}

func (c *Clock) Now() time.Time                               { return c.NowFunc() }
func (c *Clock) AfterApplyInterval() <-chan chan struct{}     { return c.AfterApplyIntervalFunc() }
func (c *Clock) AfterElectionTimeout() <-chan chan struct{}   { return c.AfterElectionTimeoutFunc() }
func (c *Clock) AfterHeartbeatInterval() <-chan chan struct{} { return c.AfterHeartbeatIntervalFunc() }
func (c *Clock) AfterReconnectTimeout() <-chan chan struct{}  { return c.AfterReconnectTimeoutFunc() }

func gosched() { time.Sleep(*goschedTimeout) }
