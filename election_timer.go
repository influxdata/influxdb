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

// An election timer tracks when an election term is complete. Terms can be
// an indefinite length of time and only end when the leader is idle for a
// specified duration. The timer can be reset whenever additional activity is
// made.
type ElectionTimer struct {
	C             chan time.Time
	rand          *rand.Rand
	duration      time.Duration
	internalTimer *time.Timer
	mutex         sync.Mutex
}

//------------------------------------------------------------------------------
//
// Constructors
//
//------------------------------------------------------------------------------

// Creates a new timer. Panics if a non-positive duration is used.
func NewElectionTimer(duration time.Duration) *ElectionTimer {
	if duration <= 0 {
		panic("raft.ElectionTimer: Non-positive duration not allowed")
	}
	return &ElectionTimer{
		C:        make(chan time.Time, 1),
		rand:     rand.New(rand.NewSource(time.Now().UnixNano())),
		duration: duration,
	}
}

//------------------------------------------------------------------------------
//
// Accessors
//
//------------------------------------------------------------------------------

// Retrieves the duration of the timer.
func (t *ElectionTimer) Duration() time.Duration {
	return t.duration
}

// Sets the duration of the timer.
func (t *ElectionTimer) SetDuration(duration time.Duration) {
	t.duration = duration
	t.Reset()
}

//------------------------------------------------------------------------------
//
// Methods
//
//------------------------------------------------------------------------------

// Checks if the timer is currently running.
func (t *ElectionTimer) Running() bool {
	return t.internalTimer != nil
}

// Stops the timer and closes the channel.
func (t *ElectionTimer) Stop() {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if t.internalTimer != nil {
		t.internalTimer.Stop()
		t.internalTimer = nil
	}

	if t.C != nil {
		close(t.C)
	}
}

// Stops the timer.
func (t *ElectionTimer) Pause() {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if t.internalTimer != nil {
		t.internalTimer.Stop()
		t.internalTimer = nil
	}
}

// Stops the timer if it is running and restarts it.
func (t *ElectionTimer) Reset() {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	// Stop the timer if it's already running.
	if t.internalTimer != nil {
		t.internalTimer.Stop()
	}

	// Start a timer that will go off between d and 2d (where d is the duration).
	d := t.duration + time.Duration(t.rand.Int63n(int64(t.duration)))
	t.internalTimer = time.NewTimer(d)
	go func() {
		defer func() {
			recover()
		}()

		// Retrieve the current internal timer.
		t.mutex.Lock()
		internalTimer := t.internalTimer
		t.mutex.Unlock()

		// If the timer exists then grab the value from the channel and pass it
		// through to the election timer's external channel.
		if internalTimer != nil {
			if v, ok := <-internalTimer.C; ok {
				t.C <- v
			}
		}
	}()
}
