package run

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influxdata/influxdb/v2/kit/check"
	"go.uber.org/zap"
)

const (
	msgWaitingForShardEnumeration = "waiting for shard enumeration"
	msgShardLoadingFailedFmt      = "shard loading failed: %s"
	msgLoadingShardsFmt           = "loading shards %.1f%% (%d / %d)"
	msgStartupReadyFmt            = "ready: %d shards loaded in %s"
	msgShardLoadFailedCountFmt    = "%d shard(s) failed to load: %s"
	msgShardLoadEntryFmt          = "shard %d: %s"

	// The ready check reports shard-loading progress under
	// measureGroupProgress, and the health check the count of failed
	// shards under measureGroupFailures, both in measureUnitShards.
	measureGroupProgress = "progress"
	measureGroupFailures = "failures"
	measureKeyCompleted  = "completed"
	measureKeyTotal      = "total"
	measureKeyCount      = "count"
	measureUnitShards    = "shards"
)

type shardLoadError struct {
	shardID uint64
	msg     string
}

// StartupProgressLogger tracks shard-loading progress for the /ready
// endpoint and accumulates per-shard load errors for the /health
// endpoint. The same instance backs both checkers; ReadyChecker and
// HealthChecker return Checkers sharing the same name.
type StartupProgressLogger struct {
	name string

	shardsCompleted atomic.Uint64
	shardsTotal     atomic.Uint64
	done            atomic.Bool
	failErrMsg      atomic.Pointer[string]
	startTime       time.Time
	logger          *zap.Logger

	shardLoadMu   sync.RWMutex
	shardLoadErrs []shardLoadError
}

// NewStartupProgressLogger returns a logger whose ReadyChecker and
// HealthChecker both report under the given name.
func NewStartupProgressLogger(name string, logger *zap.Logger) *StartupProgressLogger {
	return &StartupProgressLogger{
		name:      name,
		startTime: time.Now(),
		logger:    logger,
	}
}

func (s *StartupProgressLogger) AddShard() {
	s.shardsTotal.Add(1)
}

func (s *StartupProgressLogger) CompletedShard() {
	shardsCompleted := s.shardsCompleted.Add(1)
	totalShards := s.shardsTotal.Load()

	if totalShards == 0 {
		// Defensive: callers should enumerate via AddShard before reporting
		// completions. Avoid emitting "Inf%" if the contract is violated.
		s.logger.Info(fmt.Sprintf("Finished loading shard (%d completed, total unknown).", shardsCompleted))
		return
	}
	percentShards := float64(shardsCompleted) / float64(totalShards) * 100
	s.logger.Info(fmt.Sprintf("Finished loading shard, current progress %.1f%% shards (%d / %d).", percentShards, shardsCompleted, totalShards))
}

// ShardLoadFailed records that the shard with the given id failed to
// load. All reported failures accumulate; HealthChecker reports them
// all. ReadyChecker is unaffected — readiness is determined solely by
// the engine.Open outcome reported via Finish.
func (s *StartupProgressLogger) ShardLoadFailed(shardID uint64, err error) {
	s.shardLoadMu.Lock()
	s.shardLoadErrs = append(s.shardLoadErrs, shardLoadError{shardID: shardID, msg: err.Error()})
	s.shardLoadMu.Unlock()
}

// Finish marks the startup progress as complete. If err is nil, the
// ReadyChecker reports StatusPass. If err is non-nil, the ReadyChecker
// reports a terminal StatusFail surfacing the error message. It is safe
// to call Finish(nil) before any shards are enumerated (covers the
// fresh-install / zero-shard path).
func (s *StartupProgressLogger) Finish(err error) {
	if err != nil {
		msg := err.Error()
		// Store the message before flipping done so the ReadyChecker
		// observing done==true is guaranteed to see the failure reason.
		s.failErrMsg.Store(&msg)
	}
	s.done.Store(true)
}

// ReadyChecker returns a Checker for /ready. It reports StatusPass once
// Finish(nil) has been called, StatusFail with the error message after
// Finish(err) with a non-nil err, and a percentage-loaded StatusFail
// before Finish.
func (s *StartupProgressLogger) ReadyChecker() check.NamedChecker {
	return startupChecker{name: s.name, fn: s.checkReady}
}

// HealthChecker returns a Checker for /health. It returns Pass until at
// least one ShardLoadFailed has been recorded; thereafter it returns
// Fail with a message listing every accumulated shard load error.
func (s *StartupProgressLogger) HealthChecker() check.NamedChecker {
	return startupChecker{name: s.name, fn: s.checkHealth}
}

func (s *StartupProgressLogger) checkReady(_ context.Context) check.Response {
	// Load done before the counters: every CompletedShard precedes Finish,
	// so counters read after observing done are final.
	done := s.done.Load()
	completed := s.shardsCompleted.Load()
	total := s.shardsTotal.Load()
	progress := check.Measure{Unit: measureUnitShards, Values: map[string]float64{
		measureKeyCompleted: float64(completed),
		measureKeyTotal:     float64(total),
	}}
	if done {
		if msg := s.failErrMsg.Load(); msg != nil {
			return check.Fail(fmt.Sprintf(msgShardLoadingFailedFmt, *msg)).
				WithMeasure(measureGroupProgress, progress)
		}
		// No progress on the pass response: /ready lists only failing
		// checks, so it is never served.
		return check.Info(msgStartupReadyFmt,
			completed,
			time.Since(s.startTime).Round(time.Second))
	}
	if total == 0 {
		return check.Fail(msgWaitingForShardEnumeration).
			WithMeasure(measureGroupProgress, progress)
	}
	pct := float64(completed) / float64(total) * 100
	return check.Fail(fmt.Sprintf(msgLoadingShardsFmt, pct, completed, total)).
		WithMeasure(measureGroupProgress, progress)
}

func (s *StartupProgressLogger) checkHealth(_ context.Context) check.Response {
	s.shardLoadMu.RLock()
	errs := append([]shardLoadError(nil), s.shardLoadErrs...)
	s.shardLoadMu.RUnlock()
	failures := check.Measure{Unit: measureUnitShards, Values: map[string]float64{
		measureKeyCount: float64(len(errs)),
	}}
	if len(errs) == 0 {
		return check.Pass().WithMeasure(measureGroupFailures, failures)
	}
	parts := make([]string, len(errs))
	for i, e := range errs {
		parts[i] = fmt.Sprintf(msgShardLoadEntryFmt, e.shardID, e.msg)
	}
	return check.Fail(fmt.Sprintf(msgShardLoadFailedCountFmt, len(errs), strings.Join(parts, "; "))).
		WithMeasure(measureGroupFailures, failures)
}

// startupChecker pairs a fixed name with a check function so
// StartupProgressLogger can expose two NamedCheckers (ready and health)
// from a single underlying state.
type startupChecker struct {
	name string
	fn   func(context.Context) check.Response
}

func (c startupChecker) CheckName() string { return c.name }
func (c startupChecker) Check(ctx context.Context) check.Response {
	// Stamp c.name onto whatever fn returns. Rename handles both
	// BasicResponse (by-value) and stateful Responses (wrapped).
	return check.Rename(c.fn(ctx), c.name)
}
