package tsm1_test

import (
	"sync"
	"testing"
	"testing/quick"
	"time"

	"github.com/influxdb/influxdb/tsdb/engine/tsm1"
)

func TestWriteLock_FullCover(t *testing.T) {
	w := &tsm1.WriteLock{}
	w.LockRange(2, 10)

	lock := make(chan bool)
	timeout := time.NewTimer(10 * time.Millisecond)
	go func() {
		w.LockRange(1, 11)
		lock <- true
	}()
	select {
	case <-lock:
		t.Fatal("able to get lock when we shouldn't")
	case <-timeout.C:
		// we're all good
	}
}

func TestWriteLock_RightIntersect(t *testing.T) {
	w := &tsm1.WriteLock{}
	w.LockRange(2, 10)

	lock := make(chan bool)
	timeout := time.NewTimer(10 * time.Millisecond)
	go func() {
		w.LockRange(5, 15)
		lock <- true
	}()
	select {
	case <-lock:
		t.Fatal("able to get lock when we shouldn't")
	case <-timeout.C:
		// we're all good
	}
}

func TestWriteLock_LeftIntersect(t *testing.T) {
	w := &tsm1.WriteLock{}
	w.LockRange(1, 4)

	lock := make(chan bool)
	timeout := time.NewTimer(10 * time.Millisecond)
	go func() {
		w.LockRange(1, 11)
		lock <- true
	}()
	select {
	case <-lock:
		t.Fatal("able to get lock when we shouldn't")
	case <-timeout.C:
		// we're all good
	}
}

func TestWriteLock_Inside(t *testing.T) {
	w := &tsm1.WriteLock{}
	w.LockRange(4, 8)

	lock := make(chan bool)
	timeout := time.NewTimer(10 * time.Millisecond)
	go func() {
		w.LockRange(1, 11)
		lock <- true
	}()
	select {
	case <-lock:
		t.Fatal("able to get lock when we shouldn't")
	case <-timeout.C:
		// we're all good
	}
}

func TestWriteLock_Same(t *testing.T) {
	w := &tsm1.WriteLock{}
	w.LockRange(2, 10)

	lock := make(chan bool)
	timeout := time.NewTimer(10 * time.Millisecond)
	go func() {
		w.LockRange(2, 10)
		lock <- true
	}()
	select {
	case <-lock:
		t.Fatal("able to get lock when we shouldn't")
	case <-timeout.C:
		// we're all good
	}
}

// func TestWriteLock_FreeRangeWithContentionElsewhere(t *testing.T) {
// 	w := &tsm1.WriteLock{}
// 	w.LockRange(2, 10)

// 	lock := make(chan bool)
// 	freeRange := make(chan bool)
// 	timeout := time.NewTimer(10 * time.Millisecond)
// 	var wg sync.WaitGroup

// 	wg.Add(1)
// 	go func() {
// 		wg.Done()
// 		w.LockRange(4, 12)
// 		lock <- true
// 	}()

// 	// make sure the other go func has gotten to the point of requesting the lock
// 	wg.Wait()
// 	go func() {
// 		w.LockRange(15, 23)
// 		freeRange <- true
// 	}()
// 	select {
// 	case <-lock:
// 		t.Fatal("able to get lock when we shouldn't")
// 	case <-timeout.C:
// 		t.Fatal("unable to get lock of free range when contention exists elsewhere")
// 	case <-freeRange:
// 		// we're all good
// 	}
// }

func TestWriteLock_Quick(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode")
	}

	quick.Check(func(extents []struct{ Min, Max uint64 }) bool {
		var wg sync.WaitGroup
		var mu tsm1.WriteLock
		for _, extent := range extents {
			// Limit range.
			extent.Min %= 10
			extent.Max %= 10

			// Reverse if out of order.
			if extent.Min > extent.Max {
				extent.Min, extent.Max = extent.Max, extent.Min
			}

			// Lock, wait and unlock in a separate goroutine.
			wg.Add(1)
			go func(min, max int64) {
				defer wg.Done()
				mu.LockRange(min, max)
				time.Sleep(1 * time.Millisecond)
				mu.UnlockRange(min, max)
			}(int64(extent.Min), int64(extent.Max))
		}

		// All locks should return.
		wg.Wait()
		return true
	}, nil)
}
