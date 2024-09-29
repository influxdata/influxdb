package run

import (
	"fmt"
	"sync/atomic"

	"go.uber.org/zap"
)

type StartupProgressLogger struct {
	shardsCompleted atomic.Uint64
	shardsTotal     atomic.Uint64
	logger          *zap.Logger
}

func NewStartupProgressLogger(logger *zap.Logger) *StartupProgressLogger {
	return &StartupProgressLogger{
		logger: logger,
	}
}

func (s *StartupProgressLogger) AddShard() {
	s.shardsTotal.Add(1)
}

func (s *StartupProgressLogger) CompletedShard() {
	shardsCompleted := s.shardsCompleted.Add(1)
	totalShards := s.shardsTotal.Load()

	percentShards := float64(shardsCompleted) / float64(totalShards) * 100
	s.logger.Info(fmt.Sprintf("Finished loading shard, current progress %.1f%% shards (%d / %d).", percentShards, shardsCompleted, totalShards))
}
