package wg_timeout

import (
	"sync"
	"time"
)

func WaitGroupTimeout(wg *sync.WaitGroup, timeout time.Duration, emitter func()) {
	c := make(chan struct{})

	go func() {
		defer close(c)
		wg.Wait()
	}()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-c:
		break
	case <-timer.C:
		emitter()
	}
}
