package wg_timeout

import (
	"sync"
	"time"
)

func WaitGroupTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	c := make(chan struct{})

	go func() {
		defer close(c)
		wg.Wait()
	}()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-c:
		return false
	case <-timer.C:
		return true
	}
}
