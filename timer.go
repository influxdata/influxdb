package raft

import (
	"math/rand"
	"sync"
	"time"
)

//------------------------------------------------------------------------------
//
// Typedefs
//
//------------------------------------------------------------------------------

type Timer struct {
	fire  chan time.Time
	stop  chan bool
	state int

	rand          *rand.Rand
	minDuration   time.Duration
	maxDuration   time.Duration
	internalTimer *time.Timer

	mutex sync.Mutex
}

const (
	STOPPED = iota
	READY
	RUNNING
)

//------------------------------------------------------------------------------
//
// Constructors
//
//------------------------------------------------------------------------------

// Creates a new timer. Panics if a non-positive duration is used.
func NewTimer(minDuration time.Duration, maxDuration time.Duration) *Timer {
	if minDuration <= 0 {
		panic("raft: Non-positive minimum duration not allowed")
	} else if maxDuration <= 0 {
		panic("raft: Non-positive maximum duration not allowed")
	} else if minDuration > maxDuration {
		panic("raft: Minimum duration cannot be greater than maximum duration")
	}

	return &Timer{
		minDuration: minDuration,
		maxDuration: maxDuration,
		state:       READY,
		rand:        rand.New(rand.NewSource(time.Now().UnixNano())),
		stop:        make(chan bool, 1),
		fire:        make(chan time.Time),
	}
}

//------------------------------------------------------------------------------
//
// Accessors
//
//------------------------------------------------------------------------------

// Retrieves the minimum duration of the timer.
func (t *Timer) MinDuration() time.Duration {
	return t.minDuration
}

// Sets the minimum duration of the timer.
func (t *Timer) SetMinDuration(duration time.Duration) {
	t.minDuration = duration
}

// Retrieves the maximum duration of the timer.
func (t *Timer) MaxDuration() time.Duration {
	return t.maxDuration
}

// Sets the maximum duration of the timer.
func (t *Timer) SetMaxDuration(duration time.Duration) {
	t.maxDuration = duration
}

// Sets the minimum and maximum duration of the timer.
func (t *Timer) SetDuration(duration time.Duration) {
	t.minDuration = duration
	t.maxDuration = duration
}

//------------------------------------------------------------------------------
//
// Methods
//
//------------------------------------------------------------------------------

// Checks if the timer is currently running.
func (t *Timer) Running() bool {
	return t.state == RUNNING
}

// Stops the timer and closes the channel.
func (t *Timer) Stop() {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if t.internalTimer != nil {
		t.internalTimer.Stop()
	}

	if t.state != STOPPED {
		t.state = STOPPED

		// non-blocking buffer
		t.stop <- true
	}
}

// Change the state of timer to ready
func (t *Timer) Ready() {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if t.state == RUNNING {
		panic("Timer is already running")
	}
	t.state = READY
	t.stop = make(chan bool, 1)
	t.fire = make(chan time.Time)
}

// Fire at the timer
func (t *Timer) Fire() {
	select {
	case t.fire <- time.Now():
		return
	default:
		return
	}
}

// Start the timer, this func will be blocked until the timer:
//   (1) times out
//   (2) stopped
//   (3) fired
// Return false if stopped.
// Make sure the start func will not restart the stopped timer.
func (t *Timer) Start() bool {
	t.mutex.Lock()

	if t.state != READY {
		t.mutex.Unlock()
		return false
	}
	t.state = RUNNING

	d := t.minDuration

	if t.maxDuration > t.minDuration {
		d += time.Duration(t.rand.Int63n(int64(t.maxDuration - t.minDuration)))
	}

	t.internalTimer = time.NewTimer(d)
	internalTimer := t.internalTimer

	t.mutex.Unlock()

	// Wait for the timer channel, stop channel or fire channel.
	stopped := false
	select {
	case <-internalTimer.C:
	case <-t.fire:
	case <-t.stop:
		stopped = true
	}

	// Clean up timer and state.
	t.mutex.Lock()
	t.internalTimer.Stop()
	t.internalTimer = nil
	if stopped {
		t.state = STOPPED
	} else if t.state == RUNNING {
		t.state = READY
	}
	t.mutex.Unlock()

	return !stopped
}
