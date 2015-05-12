package raft_test

import (
	"flag"
	"testing"
	"time"

	"github.com/influxdb/influxdb/raft"
)

var (
	goschedTimeout = flag.Duration("gosched", 100*time.Millisecond, "gosched() delay")
)

// DefaultTime represents the time that the test clock is initialized to.
// Defaults to midnight on Jan 1, 2000 UTC
var DefaultTime = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

// Ensure the AfterApplyInterval returns a channel that fires after the default apply interval.
func TestClock_AfterApplyInterval(t *testing.T) {
	c := raft.NewClock()
	c.ApplyInterval = 10 * time.Millisecond
	t0 := time.Now()
	<-c.ApplyTimer().C()
	if d := time.Since(t0); d < c.ApplyInterval {
		t.Fatalf("channel fired too soon: %v", d)
	}
}

// Ensure the AfterElectionTimeout returns a channel that fires after the clock's election timeout.
func TestClock_AfterElectionTimeout(t *testing.T) {
	c := raft.NewClock()
	c.ElectionTimeout = 10 * time.Millisecond
	t0 := time.Now()
	<-c.ElectionTimer().C()
	if d := time.Since(t0); d < c.ElectionTimeout {
		t.Fatalf("channel fired too soon: %v", d)
	}
}

// Ensure the AfterHeartbeatInterval returns a channel that fires after the clock's heartbeat interval.
func TestClock_AfterHeartbeatInterval(t *testing.T) {
	c := raft.NewClock()
	c.HeartbeatInterval = 10 * time.Millisecond
	t0 := time.Now()
	<-c.HeartbeatTimer().C()
	if d := time.Since(t0); d < c.HeartbeatInterval {
		t.Fatalf("channel fired too soon: %v", d)
	}
}

// Ensure the AfterReconnectTimeout returns a channel that fires after the clock's reconnect interval.
func TestClock_AfterReconnectTimeout(t *testing.T) {
	c := raft.NewClock()
	c.ReconnectTimeout = 10 * time.Millisecond
	t0 := time.Now()
	<-c.ReconnectTimer().C()
	if d := time.Since(t0); d < c.ReconnectTimeout {
		t.Fatalf("channel fired too soon: %v", d)
	}
}

// Ensure the clock can return the current time.
func TestClock_Now(t *testing.T) {
	now := raft.NewClock().Now()
	if exp := time.Now(); exp.Sub(now) > 1*time.Second {
		t.Fatalf("clock time is different than wall time: exp=%v, got=%v", exp, now)
	}
}

// Clock represents a testable clock.
type Clock struct {
	now           time.Time
	applyChan     chan chan struct{}
	electionChan  chan chan struct{}
	heartbeatChan chan chan struct{}
	reconnectChan chan chan struct{}

	NowFunc            func() time.Time
	ApplyTimerFunc     func() raft.Timer
	ElectionTimerFunc  func() raft.Timer
	HeartbeatTimerFunc func() raft.Timer
	ReconnectTimerFunc func() raft.Timer
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
	c.ApplyTimerFunc = func() raft.Timer { return &timer{c: c.applyChan} }
	c.ElectionTimerFunc = func() raft.Timer { return &timer{c: c.electionChan} }
	c.HeartbeatTimerFunc = func() raft.Timer { return &timer{c: c.heartbeatChan} }
	c.ReconnectTimerFunc = func() raft.Timer { return &timer{c: c.reconnectChan} }
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

func (c *Clock) Now() time.Time             { return c.NowFunc() }
func (c *Clock) ApplyTimer() raft.Timer     { return c.ApplyTimerFunc() }
func (c *Clock) ElectionTimer() raft.Timer  { return c.ElectionTimerFunc() }
func (c *Clock) HeartbeatTimer() raft.Timer { return c.HeartbeatTimerFunc() }
func (c *Clock) ReconnectTimer() raft.Timer { return c.ReconnectTimerFunc() }

type timer struct {
	c chan chan struct{}
}

func (t *timer) C() <-chan chan struct{} {
	return t.c
}

func (t *timer) Stop() {}

func gosched() { time.Sleep(*goschedTimeout) }
