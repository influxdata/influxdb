package tsm1_test

import (
	// "sync"
	"testing"
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
