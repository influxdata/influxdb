package wg_timeout

import (
	"time"
)

func WaitGroupTimeout(breakout <-chan struct{}, timeout time.Duration, emitter func()) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-breakout:
		return
	case <-timer.C:
		emitter()
	}
}
