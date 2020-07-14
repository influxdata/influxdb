package stopwatch

import "time"

// A stopwatch that accumulates repeated calls to Start and Stop
type Timer struct {
	d     time.Duration // total duration of timer
	start time.Time
}

// Start begins the timer at the current time.
//
// If already started, does nothing.
func (t *Timer) Start() {
	if !t.start.IsZero() {
		return
	}
	t.start = time.Now()
}

// Stop ends the current measurement and adds the
// elapsed duration to t.
//
// If not paired with a call to Start, Stop does nothing.
func (t *Timer) Stop() {
	if t.start.IsZero() {
		return
	}

	t.d += time.Since(t.start)
	t.start = time.Time{}
}

// Measure adds the time spent executing fn to t.
func (t *Timer) Measure(fn func()) {
	t.Start()
	defer t.Stop()
	fn()
}

// Elapsed returns the accumulated time of t up to the last
// call to Stop.
func (t *Timer) Elapsed() time.Duration {
	return t.d
}

// IsStarted returns true if Start has been called
// and is actively measuring.
func (t *Timer) IsStarted() bool {
	return !t.start.IsZero()
}
