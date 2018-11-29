package metrics

import (
	"sync/atomic"
	"time"
)

// The timer type is used to store a duration.
type Timer struct {
	val  int64
	desc *desc
}

// Name returns the name of the timer.
func (t *Timer) Name() string { return t.desc.Name }

// Value atomically returns the value of the timer.
func (t *Timer) Value() time.Duration { return time.Duration(atomic.LoadInt64(&t.val)) }

// Update sets the timer value to d.
func (t *Timer) Update(d time.Duration) { atomic.StoreInt64(&t.val, int64(d)) }

// UpdateSince sets the timer value to the difference between since and the current time.
func (t *Timer) UpdateSince(since time.Time) { t.Update(time.Since(since)) }

// String returns a string representation using the name and value of the timer.
func (t *Timer) String() string { return t.desc.Name + ": " + time.Duration(t.val).String() }

// Time updates the timer to the duration it takes to call f.
func (t *Timer) Time(f func()) {
	s := time.Now()
	f()
	t.UpdateSince(s)
}
