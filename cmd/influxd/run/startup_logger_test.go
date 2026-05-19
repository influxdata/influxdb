package run

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/influxdata/influxdb/v2/kit/check"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

const testCheckName = "shards"

func newTestProgressLogger(t *testing.T) *StartupProgressLogger {
	return NewStartupProgressLogger(testCheckName, zaptest.NewLogger(t))
}

func TestStartupProgressLogger_CheckersShareName(t *testing.T) {
	s := newTestProgressLogger(t)

	ready := s.ReadyChecker()
	require.Equal(t, testCheckName, ready.CheckName())

	health := s.HealthChecker()
	require.Equal(t, testCheckName, health.CheckName())
}

func TestStartupProgressLogger_ReadyCheckWaiting(t *testing.T) {
	s := newTestProgressLogger(t)

	resp := s.ReadyChecker().Check(context.Background())
	require.Equal(t, check.StatusFail, resp.Status())
	require.Equal(t, msgWaitingForShardEnumeration, resp.Message())
}

func TestStartupProgressLogger_ReadyCheckInProgress(t *testing.T) {
	s := newTestProgressLogger(t)

	for range 200 {
		s.AddShard()
	}
	for range 94 {
		s.CompletedShard()
	}

	resp := s.ReadyChecker().Check(context.Background())
	require.Equal(t, check.StatusFail, resp.Status())
	require.Equal(t, fmt.Sprintf(msgLoadingShardsFmt, 47.0, 94, 200), resp.Message())
}

func TestStartupProgressLogger_ReadyCheckFinishBeforeEnumeration(t *testing.T) {
	// Fresh-install / zero-shard case: Finish(nil) flips to Pass even when
	// no shards were ever enumerated.
	s := newTestProgressLogger(t)
	s.Finish(nil)

	resp := s.ReadyChecker().Check(context.Background())
	require.Equal(t, check.StatusPass, resp.Status())
	// Duration is non-deterministic; assert prefix only.
	require.Contains(t, resp.Message(), "ready: 0 shards loaded in ")
}

func TestStartupProgressLogger_ReadyCheckFinishAfterLoading(t *testing.T) {
	s := newTestProgressLogger(t)
	for range 3 {
		s.AddShard()
		s.CompletedShard()
	}
	s.Finish(nil)

	resp := s.ReadyChecker().Check(context.Background())
	require.Equal(t, check.StatusPass, resp.Status())
	require.Contains(t, resp.Message(), "ready: 3 shards loaded in ")
}

func TestStartupProgressLogger_ReadyCheckFinishWithError(t *testing.T) {
	s := newTestProgressLogger(t)
	s.AddShard()
	s.Finish(errors.New("disk on fire"))

	resp := s.ReadyChecker().Check(context.Background())
	require.Equal(t, check.StatusFail, resp.Status())
	require.Equal(t, fmt.Sprintf(msgShardLoadingFailedFmt, "disk on fire"), resp.Message())
}

// Ready remains Pass even when individual shards failed, as long as
// engine.Open returned nil and Finish(nil) was called.
func TestStartupProgressLogger_ReadyCheckPassesDespiteShardFailures(t *testing.T) {
	s := newTestProgressLogger(t)
	s.AddShard()
	s.CompletedShard()
	s.ShardLoadFailed(42, errors.New("corrupt index"))
	s.Finish(nil)

	resp := s.ReadyChecker().Check(context.Background())
	require.Equal(t, check.StatusPass, resp.Status())
}

func TestStartupProgressLogger_HealthCheckPassesByDefault(t *testing.T) {
	s := newTestProgressLogger(t)

	resp := s.HealthChecker().Check(context.Background())
	require.Equal(t, check.StatusPass, resp.Status())
}

func TestStartupProgressLogger_HealthCheckReportsAllFailures(t *testing.T) {
	s := newTestProgressLogger(t)
	s.ShardLoadFailed(7, errors.New("corrupt index"))
	s.ShardLoadFailed(42, errors.New("missing series file"))
	s.ShardLoadFailed(99, errors.New("bad TSM"))

	resp := s.HealthChecker().Check(context.Background())
	require.Equal(t, check.StatusFail, resp.Status())
	require.Contains(t, resp.Message(), "3 shard(s) failed to load")
	require.Contains(t, resp.Message(), "shard 7: corrupt index")
	require.Contains(t, resp.Message(), "shard 42: missing series file")
	require.Contains(t, resp.Message(), "shard 99: bad TSM")
}

// HealthChecker tracks shard failures independently of Finish — it
// reports them whether or not engine.Open eventually succeeds.
func TestStartupProgressLogger_HealthCheckIndependentOfFinish(t *testing.T) {
	s := newTestProgressLogger(t)
	s.ShardLoadFailed(1, errors.New("boom"))
	s.Finish(nil) // engine.Open returned nil

	resp := s.HealthChecker().Check(context.Background())
	require.Equal(t, check.StatusFail, resp.Status())
	require.Contains(t, resp.Message(), "shard 1: boom")
}

func TestStartupProgressLogger_CompletedShardBeforeAddShard(t *testing.T) {
	// If CompletedShard ever runs before AddShard, the percentage would
	// otherwise be +Inf. Verify the defensive guard keeps things sane.
	s := newTestProgressLogger(t)
	s.CompletedShard() // must not panic or emit Inf
	require.Equal(t, uint64(1), s.shardsCompleted.Load())
}

// TestStartupProgressLogger_ConcurrentReadersAndLoaders exercises the
// lock-free Check path against concurrent AddShard / CompletedShard
// calls, plus concurrent ShardLoadFailed reporters and HealthChecker
// readers. Uses the RWMutex start-gate pattern so every goroutine
// contends simultaneously; failures appear under `go test -race`.
func TestStartupProgressLogger_ConcurrentReadersAndLoaders(t *testing.T) {
	const (
		numLoaders      = 16
		numReaders      = 16
		numFailers      = 8
		shardsPerLoader = 64
		failsPerFailer  = 8
	)

	s := newTestProgressLogger(t)
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
		failers        sync.WaitGroup
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

	for i := range numFailers {
		failers.Add(1)
		go func(idx int) {
			startMu.RLock()
			defer startMu.RUnlock()
			defer failers.Done()
			cur := concurrency.Add(1)
			for {
				old := maxConcurrency.Load()
				if cur <= old || maxConcurrency.CompareAndSwap(old, cur) {
					break
				}
			}
			for j := range failsPerFailer {
				s.ShardLoadFailed(uint64(idx*failsPerFailer+j), errors.New("boom"))
			}
			concurrency.Add(-1)
		}(i)
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
			ready := s.ReadyChecker()
			health := s.HealthChecker()
			for !readerStop.Load() {
				// Before Finish, ready is always Fail. Health may be
				// Pass or Fail depending on whether any failer has
				// reported; both are valid mid-test states.
				assert.Equal(t, check.StatusFail, ready.Check(context.Background()).Status())
				_ = health.Check(context.Background())
			}
			concurrency.Add(-1)
		}()
	}

	startMu.Unlock()

	loaders.Wait()
	failers.Wait()
	readerStop.Store(true)
	readers.Wait()

	t.Logf("max concurrency: %d", maxConcurrency.Load())

	// Every shard completed exactly once.
	require.Equal(t, uint64(numLoaders*shardsPerLoader), s.shardsCompleted.Load())
	require.Len(t, s.shardLoadErrs, numFailers*failsPerFailer)

	// After Finish(nil), ReadyChecker flips to Pass; HealthChecker still
	// reflects the recorded shard failures.
	s.Finish(nil)
	require.Equal(t, check.StatusPass, s.ReadyChecker().Check(context.Background()).Status())
	require.Equal(t, check.StatusFail, s.HealthChecker().Check(context.Background()).Status())
}
