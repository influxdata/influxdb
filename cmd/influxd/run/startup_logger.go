package run

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/influxdata/influxdb/v2/kit/check"
	"go.uber.org/zap"
)

const (
	// ShardsCheckName is the name surfaced on /ready for the shard-loading
	// progress check.
	ShardsCheckName = "shards"

	msgWaitingForShardEnumeration = "waiting for shard enumeration"
	msgShardLoadingFailedFmt      = "shard loading failed: %s"
	msgLoadingShardsFmt           = "loading shards %.1f%% (%d / %d)"
	msgStartupReadyFmt            = "ready: %d shards loaded in %s"
)

type StartupProgressLogger struct {
	shardsCompleted atomic.Uint64
	shardsTotal     atomic.Uint64
	done            atomic.Bool
	failErrMsg      atomic.Pointer[string]
	startTime       time.Time
	logger          *zap.Logger
}

func NewStartupProgressLogger(logger *zap.Logger) *StartupProgressLogger {
	return &StartupProgressLogger{
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

// Finish marks the startup progress as complete. If err is nil, Check
// reports StatusPass. If err is non-nil, Check reports a terminal StatusFail
// surfacing the error message. It is safe to call Finish(nil) before any
// shards are enumerated (covers the fresh-install / zero-shard path).
func (s *StartupProgressLogger) Finish(err error) {
	if err != nil {
		msg := err.Error()
		// Store the message before flipping done so a Check observing
		// done==true is guaranteed to see the failure reason.
		s.failErrMsg.Store(&msg)
	}
	s.done.Store(true)
}

// CheckName returns the name used when this logger is registered as a
// check.NamedChecker.
func (*StartupProgressLogger) CheckName() string { return ShardsCheckName }

// Check reports readiness. After Finish(nil) it returns StatusPass; after
// Finish(err) with a non-nil err it returns a terminal StatusFail with the
// error message. Before Finish it returns StatusFail with a percentage-loaded
// message, or msgWaitingForShardEnumeration if the shard count has not yet
// been established.
func (s *StartupProgressLogger) Check(_ context.Context) check.Response {
	if s.done.Load() {
		if msg := s.failErrMsg.Load(); msg != nil {
			return check.Response{
				Status:  check.StatusFail,
				Message: fmt.Sprintf(msgShardLoadingFailedFmt, *msg),
			}
		}
		return check.Info(msgStartupReadyFmt,
			s.shardsCompleted.Load(),
			time.Since(s.startTime).Round(time.Second))
	}
	completed := s.shardsCompleted.Load()
	total := s.shardsTotal.Load()
	if total == 0 {
		return check.Response{Status: check.StatusFail, Message: msgWaitingForShardEnumeration}
	}
	pct := float64(completed) / float64(total) * 100
	return check.Response{
		Status:  check.StatusFail,
		Message: fmt.Sprintf(msgLoadingShardsFmt, pct, completed, total),
	}
}
