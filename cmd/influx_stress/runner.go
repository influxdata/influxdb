package runner

import (
	"time"
)

type Timer struct {
	start time.Time
	end   time.Time
}

func (t *Timer) Start() {
	t.start = time.Now()
}

func (t *Timer) Stop() {
	t.end = time.Now()
}

func (t *Timer) elapsed() time.Duration {
	return t.end.Sub(t.start)
}

func newTimer() *Timer {
	t := &Timer{}
	t.Start()
	return t
}
