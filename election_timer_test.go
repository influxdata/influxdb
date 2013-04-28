package raft

import (
	"testing"
	"time"
)

//------------------------------------------------------------------------------
//
// Tests
//
//------------------------------------------------------------------------------

// Ensure that we can start an election timer and it will go off in the specified duration.
func TestElectionTimerReset(t *testing.T) {
	count, finished := 0, false
	timer := NewElectionTimer(5 * time.Millisecond)
	go func() {
		for {
			if _, ok := <-timer.C; ok {
				count++
				timer.Reset()
			} else {
				break
			}
		}
		finished = true
	}()
	timer.Reset()
	time.Sleep(25 * time.Millisecond)
	if count < 2 {
		t.Fatalf("Timer should have executed at least twice (%d)", count)
	}
	if finished {
		t.Fatalf("Timer finished too early")
	}

	timer.Stop()

	// Uncomment below to test timer stops. Golang channel closing is unpredictable though so it slows down our test.
	/**
	time.Sleep(500 * time.Millisecond)
	if !finished {
		t.Fatalf("Timer did not finish")
	}
	*/
}

// Ensure that we can pause an election timer.
func TestElectionTimerPause(t *testing.T) {
	count := 0
	timer := NewElectionTimer(10 * time.Millisecond)
	go func() {
		<-timer.C
		count++
	}()
	timer.Reset()

	time.Sleep(5 * time.Millisecond)
	if count != 0 {
		t.Fatalf("Timer executed when it should not have (%d)", count)
	}
	timer.Pause()

	time.Sleep(20 * time.Millisecond)
	if count != 0 {
		t.Fatalf("Timer executed when it should not have (%d)", count)
	}

	timer.Reset()
	time.Sleep(50 * time.Millisecond)
	if count != 1 {
		t.Fatalf("Timer did not execute when it should have (%d)", count)
	}

	timer.Stop()
}
