package check

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCheck_ConcurrentRegistrationAndEvaluation exercises the RWMutex
// protecting Check's healthChecks and readyChecks slices: N goroutines
// register checkers while N goroutines concurrently call CheckHealth and
// CheckReady. Under -race this fails without the mutex.
//
// Uses the RWMutex start-gate pattern to start all goroutines
// contending simultaneously.
func TestCheck_ConcurrentRegistrationAndEvaluation(t *testing.T) {
	const (
		numRegisterers = 16
		numEvaluators  = 16
		numChecksEach  = 32
		numEvaluations = 64

		healthName = "h"
		readyName  = "r"
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

	for range numRegisterers {
		wg.Add(1)
		go func() {
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
			for i := range numChecksEach {
				if i%2 == 0 {
					c.AddHealthCheck(mockPass(healthName))
				} else {
					c.AddNamedReadyCheck(Named(readyName, mockPass(readyName)))
				}
			}
			concurrency.Add(-1)
		}()
	}

	for range numEvaluators {
		wg.Add(1)
		go func() {
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
			for range numEvaluations {
				c.CheckHealth(ctx)
				c.CheckReady(ctx)
			}
			concurrency.Add(-1)
		}()
	}

	startMu.Unlock()
	wg.Wait()

	t.Logf("max concurrency: %d", maxConcurrency.Load())

	// After the race settles we should have the full expected number of
	// checks registered on each slice.
	wantHealth := numRegisterers * (numChecksEach / 2)
	wantReady := numRegisterers * (numChecksEach / 2)
	resp := c.CheckHealth(ctx)
	require.Len(t, resp.Checks(), wantHealth)
	resp = c.CheckReady(ctx)
	require.Len(t, resp.Checks(), wantReady)
}

// TestCheck_ConcurrentFreeze runs freezers against evaluators and registerers,
// all released together. Freeze reads the checker slices under RLock, evaluates
// with no lock held, and installs under Lock, so it is the one operation that
// spans both locks; under -race this fails if any of the three steps touches
// the slices unguarded.
//
// The assertion that matters is after the race settles: once frozen, two
// successive evaluations must be element-wise identical. Everything the
// evaluators check while the race is on is weaker than that -- it exists to
// catch a torn install being observed mid-flight, which the settled comparison
// could not see.
func TestCheck_ConcurrentFreeze(t *testing.T) {
	const (
		numFreezers    = 4
		numRegisterers = 8
		numEvaluators  = 16
		numChecksEach  = 32
		numEvaluations = 64

		healthName = "h"
		readyName  = "r"
	)

	c := NewCheck()
	ctx := context.Background()

	// Registered up front so every freezer has something to snapshot even if it
	// wins the race against every registerer.
	c.AddNamedHealthCheck(Named(healthName, mockPass(healthName)))
	c.AddNamedReadyCheck(Named(readyName, mockPass(readyName)))

	var (
		startMu        sync.RWMutex
		concurrency    atomic.Int64
		maxConcurrency atomic.Int64
	)

	// enter blocks until the start gate opens, then records the observed
	// overlap. The returned func is deferred by the caller to leave.
	enter := func() func() {
		startMu.RLock()
		cur := concurrency.Add(1)
		for {
			old := maxConcurrency.Load()
			if cur <= old || maxConcurrency.CompareAndSwap(old, cur) {
				break
			}
		}
		return func() {
			concurrency.Add(-1)
			startMu.RUnlock()
		}
	}

	var wg sync.WaitGroup
	startMu.Lock()

	for range numFreezers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer enter()()
			c.Freeze(ctx)
		}()
	}

	for range numRegisterers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer enter()()
			for i := range numChecksEach {
				if i%2 == 0 {
					c.AddHealthCheck(mockPass(healthName))
				} else {
					c.AddNamedReadyCheck(Named(readyName, mockPass(readyName)))
				}
			}
		}()
	}

	for range numEvaluators {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer enter()()
			for range numEvaluations {
				for _, resp := range []Response{c.CheckHealth(ctx), c.CheckReady(ctx)} {
					// A torn or zero-valued frozen entry would show up as an
					// empty name or an empty status, neither of which any
					// registered checker here can produce.
					for _, sub := range resp.Checks() {
						assert.NotEmpty(t, sub.Name())
						assert.Contains(t, []Status{StatusPass, StatusFail}, sub.Status())
					}
				}
			}
		}()
	}

	startMu.Unlock()
	wg.Wait()

	t.Logf("max concurrency: %d", maxConcurrency.Load())

	// Frozen, and terminal: the set no longer moves, whichever freezer won.
	first := c.CheckHealth(ctx)
	second := c.CheckHealth(ctx)
	require.Equal(t, first.Checks(), second.Checks())
	require.Equal(t, first.Status(), second.Status())

	firstReady := c.CheckReady(ctx)
	secondReady := c.CheckReady(ctx)
	require.Equal(t, firstReady.Checks(), secondReady.Checks())
	require.Len(t, c.ReadyCheckNames(), len(firstReady.Checks()))

	// Nothing registered after the winning freeze survived, so both lists are
	// bounded by what was registered before it.
	require.LessOrEqual(t, len(first.Checks()), 1+numRegisterers*(numChecksEach/2))
	require.GreaterOrEqual(t, len(first.Checks()), 1)
}
