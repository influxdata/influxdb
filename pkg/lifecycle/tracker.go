package lifecycle

import "sync"

// Tracker helps keep track of background processes, signal them to exit
// and wait for them to finish.
type Tracker struct {
	mu   sync.RWMutex
	wg   sync.WaitGroup
	open bool
	intC chan struct{}
}

// Open starts the Tracker. It returns a channel that can be monitored for when
// the tracker is closed. This is useful for syncronously spawning tasks during
// open, as Start also returns the channel. Prefer using the channel from start
// to avoid issues where the incorrect channel is read from for cancellation.
// It should not be called concurrently with Close.
func (t *Tracker) Open() chan struct{} {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.open {
		t.open = true
		t.intC = make(chan struct{})
	}

	return t.intC
}

// Close signals to any started tasks that they should exit and waits for there
// to be no tasks. It should not be called concurrently with Open.
func (t *Tracker) Close() {
	t.mu.Lock()
	if t.open {
		t.open = false
		close(t.intC)
	}
	t.mu.Unlock()

	// It's safe to wait without the lock because we know that the only time
	// someone adds to the wait group is during startTask, which checks for
	// closed and adds to the wait group under the read lock. Thus, no adds
	// can happen after the mutex unlock until the next call to Open.
	t.wg.Wait()
}

// Start reports a channel to check for interrupts as well as a boolean indicating
// if the tracker is open.
func (t *Tracker) Start() (chan struct{}, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if !t.open {
		return nil, false
	}

	t.wg.Add(1)
	return t.intC, t.open
}

// Check reports if the tracker is open.
func (t *Tracker) Check() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.open
}

// Stop signals to the tracker that the task has finished.
func (t *Tracker) Stop() {
	t.mu.RLock()
	defer t.mu.RUnlock()

	t.wg.Done()
}
