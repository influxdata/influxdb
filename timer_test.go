package raft

import (
	"sync"
	"testing"
	"time"
)

//------------------------------------------------------------------------------
//
// Tests
//
//------------------------------------------------------------------------------

// Ensure that we can start an election timer and it will go off in the specified duration.
func TestTimerReset(t *testing.T) {
	var mutex sync.Mutex
	count, finished := 0, false
	timer := NewTimer(5*time.Millisecond, 10*time.Millisecond)
	go func() {
		for {
			if _, ok := <-timer.C(); ok {
				mutex.Lock()
				count++
				timer.Reset()
				mutex.Unlock()
			} else {
				break
			}
		}
		mutex.Lock()
		finished = true
		mutex.Unlock()
	}()
	timer.Reset()
	time.Sleep(25 * time.Millisecond)
	mutex.Lock()
	if count < 2 {
		t.Fatalf("Timer should have executed at least twice (%d)", count)
	}
	if finished {
		t.Fatalf("Timer finished too early")
	}
	mutex.Unlock()

	timer.Stop()

	// Uncomment below to test timer stops. Golang channel closing is unpredictable though so it slows down our test.
	/**
	time.Sleep(500 * time.Millisecond)
	mutex.Lock()
	defer mutex.Unlock()
	if !finished {
		t.Fatalf("Timer did not finish")
	}
	*/
}

// Ensure that we can pause an election timer.
func TestTimerPause(t *testing.T) {
	var mutex sync.Mutex
	count := 0
	timer := NewTimer(10*time.Millisecond, 20*time.Millisecond)
	go func() {
		<-timer.C()
		mutex.Lock()
		count++
		mutex.Unlock()
	}()
	timer.Reset()

	time.Sleep(5 * time.Millisecond)
	mutex.Lock()
	if count != 0 {
		t.Fatalf("Timer executed when it should not have (%d)", count)
	}
	mutex.Unlock()
	timer.Pause()

	time.Sleep(20 * time.Millisecond)
	mutex.Lock()
	if count != 0 {
		t.Fatalf("Timer executed when it should not have (%d)", count)
	}
	mutex.Unlock()

	timer.Reset()
	time.Sleep(50 * time.Millisecond)
	mutex.Lock()
	if count != 1 {
		t.Fatalf("Timer did not execute when it should have (%d)", count)
	}
	mutex.Unlock()

	timer.Stop()
}
