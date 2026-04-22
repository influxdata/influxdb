package check

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCheck_ConcurrentRegistrationAndEvaluation exercises the RWMutex
// protecting Check's healthChecks and readyChecks slices: N goroutines
// register checkers while N goroutines concurrently call CheckHealth and
// CheckReady. Under -race this fails without the mutex.
//
// Uses the RWMutex start-gate pattern from CLAUDE.md so all goroutines
// start contending simultaneously.
func TestCheck_ConcurrentRegistrationAndEvaluation(t *testing.T) {
	const (
		numRegisterers = 16
		numEvaluators  = 16
		numChecksEach  = 32
		numEvaluations = 64
	)

	c := NewCheck()
	ctx := context.Background()

	var (
		startMu        sync.RWMutex
		concurrency    atomic.Int64
		maxConcurrency atomic.Int64
	)

	var wg sync.WaitGroup
	startMu.Lock()

	for g := 0; g < numRegisterers; g++ {
		wg.Add(1)
		go func(idx int) {
			startMu.RLock()
			defer startMu.RUnlock()
			defer wg.Done()
			cur := concurrency.Add(1)
			for {
				old := maxConcurrency.Load()
				if cur <= old || maxConcurrency.CompareAndSwap(old, cur) {
					break
				}
			}
			for i := 0; i < numChecksEach; i++ {
				if i%2 == 0 {
					c.AddHealthCheck(mockPass("h"))
				} else {
					c.AddReadyCheck(mockPass("r"))
				}
			}
			concurrency.Add(-1)
		}(g)
	}

	for g := 0; g < numEvaluators; g++ {
		wg.Add(1)
		go func(idx int) {
			startMu.RLock()
			defer startMu.RUnlock()
			defer wg.Done()
			cur := concurrency.Add(1)
			for {
				old := maxConcurrency.Load()
				if cur <= old || maxConcurrency.CompareAndSwap(old, cur) {
					break
				}
			}
			for i := 0; i < numEvaluations; i++ {
				c.CheckHealth(ctx)
				c.CheckReady(ctx)
			}
			concurrency.Add(-1)
		}(g)
	}

	startMu.Unlock()
	wg.Wait()

	t.Logf("max concurrency: %d", maxConcurrency.Load())

	// After the race settles we should have the full expected number of
	// checks registered on each slice.
	wantHealth := numRegisterers * (numChecksEach / 2)
	wantReady := numRegisterers * (numChecksEach / 2)
	resp := c.CheckHealth(ctx)
	require.Len(t, resp.Checks, wantHealth)
	resp = c.CheckReady(ctx)
	require.Len(t, resp.Checks, wantReady)
}
