package run

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/influxdata/influxdb/v2/kit/check"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func TestStartupProgressLogger_CheckName(t *testing.T) {
	require.Equal(t, "shards", NewStartupProgressLogger(zaptest.NewLogger(t)).CheckName())
}

func TestStartupProgressLogger_CheckWaiting(t *testing.T) {
	s := NewStartupProgressLogger(zaptest.NewLogger(t))

	resp := s.Check(context.Background())
	require.Equal(t, check.StatusFail, resp.Status)
	require.Equal(t, "waiting for shard enumeration", resp.Message)
}

func TestStartupProgressLogger_CheckInProgress(t *testing.T) {
	s := NewStartupProgressLogger(zaptest.NewLogger(t))

	for range 200 {
		s.AddShard()
	}
	for range 94 {
		s.CompletedShard()
	}

	resp := s.Check(context.Background())
	require.Equal(t, check.StatusFail, resp.Status)
	require.Equal(t, "loading shards 47.0% (94 / 200)", resp.Message)
}

func TestStartupProgressLogger_CheckFinishBeforeEnumeration(t *testing.T) {
	// Fresh-install / zero-shard case: Finish(nil) flips to Pass even when
	// no shards were ever enumerated.
	s := NewStartupProgressLogger(zaptest.NewLogger(t))
	s.Finish(nil)

	resp := s.Check(context.Background())
	require.Equal(t, check.StatusPass, resp.Status)
	assert.Empty(t, resp.Message)
}

func TestStartupProgressLogger_CheckFinishAfterLoading(t *testing.T) {
	s := NewStartupProgressLogger(zaptest.NewLogger(t))
	for range 3 {
		s.AddShard()
		s.CompletedShard()
	}
	s.Finish(nil)

	resp := s.Check(context.Background())
	require.Equal(t, check.StatusPass, resp.Status)
}

func TestStartupProgressLogger_CheckFinishWithError(t *testing.T) {
	s := NewStartupProgressLogger(zaptest.NewLogger(t))
	s.AddShard()
	s.Finish(errors.New("disk on fire"))

	resp := s.Check(context.Background())
	require.Equal(t, check.StatusFail, resp.Status)
	require.Equal(t, "shard loading failed: disk on fire", resp.Message)
}

func TestStartupProgressLogger_CompletedShardBeforeAddShard(t *testing.T) {
	// If CompletedShard ever runs before AddShard, the percentage would
	// otherwise be +Inf. Verify the defensive guard keeps things sane.
	s := NewStartupProgressLogger(zaptest.NewLogger(t))
	s.CompletedShard() // must not panic or emit Inf
	require.Equal(t, uint64(1), s.shardsCompleted.Load())
}

// TestStartupProgressLogger_ConcurrentReadersAndLoaders exercises the
// lock-free Check path against concurrent AddShard / CompletedShard calls.
// Uses the RWMutex start-gate pattern so every goroutine
// contends simultaneously; failures appear under `go test -race`.
func TestStartupProgressLogger_ConcurrentReadersAndLoaders(t *testing.T) {
	const (
		numLoaders      = 16
		numReaders      = 16
		shardsPerLoader = 64
	)

	s := NewStartupProgressLogger(zaptest.NewLogger(t))
	// Pre-seed the total so CompletedShard's percentage computation is
	// well-defined. Loaders only call CompletedShard below.
	for range numLoaders * shardsPerLoader {
		s.AddShard()
	}

	var (
		startMu        sync.RWMutex
		concurrency    atomic.Int64
		maxConcurrency atomic.Int64
		readerStop     atomic.Bool
		loaders        sync.WaitGroup
		readers        sync.WaitGroup
	)
	startMu.Lock()

	for range numLoaders {
		loaders.Add(1)
		go func() {
			startMu.RLock()
			defer startMu.RUnlock()
			defer loaders.Done()
			cur := concurrency.Add(1)
			for {
				old := maxConcurrency.Load()
				if cur <= old || maxConcurrency.CompareAndSwap(old, cur) {
					break
				}
			}
			for range shardsPerLoader {
				s.CompletedShard()
			}
			concurrency.Add(-1)
		}()
	}

	for range numReaders {
		readers.Add(1)
		go func() {
			startMu.RLock()
			defer startMu.RUnlock()
			defer readers.Done()
			cur := concurrency.Add(1)
			for {
				old := maxConcurrency.Load()
				if cur <= old || maxConcurrency.CompareAndSwap(old, cur) {
					break
				}
			}
			for !readerStop.Load() {
				resp := s.Check(context.Background())
				// Before Finish is called, status is always Fail.
				assert.Equal(t, check.StatusFail, resp.Status)
			}
			concurrency.Add(-1)
		}()
	}

	startMu.Unlock()

	loaders.Wait()
	readerStop.Store(true)
	readers.Wait()

	t.Logf("max concurrency: %d", maxConcurrency.Load())

	// Every shard completed exactly once.
	require.Equal(t, uint64(numLoaders*shardsPerLoader), s.shardsCompleted.Load())

	// After Finish(nil), Check flips to Pass.
	s.Finish(nil)
	require.Equal(t, check.StatusPass, s.Check(context.Background()).Status)
}
