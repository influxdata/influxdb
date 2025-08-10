package wg_timeout

import (
	"sync"
	"time"
)

func WaitGroupTimeout(wg *sync.WaitGroup, timeout time.Duration, emitter func()) {
	breakoutChan := make(chan struct{})

	go func(c chan struct{}) {
		defer close(c)
		wg.Wait()
	}(breakoutChan)

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-breakoutChan:
		return
	case <-timer.C:
		emitter()
	}
}
