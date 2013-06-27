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
	STOP = iota
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
	}

	if maxDuration <= 0 {
		panic("raft: Non-positive maximum duration not allowed")
	}

	if minDuration > maxDuration {
		panic("raft: Minimum duration cannot be greater than maximum duration")
	}

	return &Timer{
		rand:        rand.New(rand.NewSource(time.Now().UnixNano())),
		minDuration: minDuration,
		maxDuration: maxDuration,
		state:       READY,
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
	if t.internalTimer != nil {
		t.internalTimer.Stop()
	}

	if t.state != STOP {
		t.state = STOP

		// non-blocking buffer

		t.stop <- true
	}

	t.mutex.Unlock()
}

// Change the state of timer to ready
func (t *Timer) Ready() {
	t.mutex.Lock()
	if t.state == RUNNING {
		panic("Timer state is Running")
	}
	t.state = READY
	t.stop = make(chan bool, 1)
	t.fire = make(chan time.Time)
	t.mutex.Unlock()
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

// start the timer, this func will be blocked
// until (1) the timer is timeout
//       (2) stopped
//       (3) fired
// reutrn false if stopped
// make sure the start func will not restart the
// stopped timer
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

	t.mutex.Unlock()

	internalTimer := t.internalTimer

	select {
	case <-internalTimer.C:
	
		t.internalTimer = nil
		t.mutex.Lock()
		if t.state == RUNNING {
			t.state = READY
		}
		t.mutex.Unlock()
		return true

	case <-t.fire:

		t.internalTimer.Stop()
		t.internalTimer = nil
		t.mutex.Lock()
		if t.state == RUNNING {
			t.state = READY
		}
		t.mutex.Unlock()
		return true

	case <-t.stop:
	
		t.internalTimer = nil
		t.mutex.Lock()
		t.state = STOP
		t.mutex.Unlock()
		return false
	}

}
