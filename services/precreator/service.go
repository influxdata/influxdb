// Package precreator provides the shard precreation service.
package precreator // import "github.com/influxdata/influxdb/services/precreator"

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/services/meta"
	"go.uber.org/zap"
)

// Service manages the shard precreation service.
type Service struct {
	checkInterval time.Duration
	advancePeriod time.Duration

	Logger *zap.Logger

	done chan struct{}
	wg   sync.WaitGroup

	Store interface {
		CreateShard(database, retentionPolicy string, shardID uint64, enabled bool) error
	}

	MetaClient interface {
		PrecreateShardGroups(now, cutoff time.Time) ([]meta.ShardGroupFullInfo, error)
	}
}

// NewService returns an instance of the precreation service.
func NewService(c Config) *Service {
	return &Service{
		checkInterval: time.Duration(c.CheckInterval),
		advancePeriod: time.Duration(c.AdvancePeriod),
		Logger:        zap.NewNop(),
	}
}

// WithLogger sets the logger for the service.
func (s *Service) WithLogger(log *zap.Logger) {
	s.Logger = log.With(zap.String("service", "shard-precreation"))
}

// Open starts the precreation service.
func (s *Service) Open() error {
	if s.done != nil {
		return nil
	}

	s.Logger.Info("Starting precreation service",
		logger.DurationLiteral("check_interval", s.checkInterval),
		logger.DurationLiteral("advance_period", s.advancePeriod))

	s.done = make(chan struct{})

	s.wg.Add(1)
	go s.runPrecreation()
	return nil
}

// Close stops the precreation service.
func (s *Service) Close() error {
	if s.done == nil {
		return nil
	}

	close(s.done)
	s.wg.Wait()
	s.done = nil

	return nil
}

// runPrecreation continually checks if resources need precreation.
func (s *Service) runPrecreation() {
	defer s.wg.Done()

	for {
		select {
		case <-time.After(s.checkInterval):
			if err := s.precreate(time.Now().UTC()); err != nil {
				s.Logger.Warn("Failed to precreate shards", zap.Error(err))
			}
		case <-s.done:
			s.Logger.Info("Terminating precreation service")
			return
		}
	}
}

// precreate performs actual resource precreation.
func (s *Service) precreate(now time.Time) error {
	cutoff := now.Add(s.advancePeriod).UTC()
	if newShardGroups, err := s.MetaClient.PrecreateShardGroups(now, cutoff); err != nil {
		return err
	} else {
		errs := make([]error, 0, len(newShardGroups))
		for _, sgfi := range newShardGroups {
			if len(sgfi.ShardGroup.Shards) <= 0 {
				err := fmt.Errorf("shard group %d covering %s to %s for database %s and retention policy %s has no shards",
					sgfi.ShardGroup.ID,
					sgfi.ShardGroup.StartTime,
					sgfi.ShardGroup.EndTime,
					sgfi.Database,
					sgfi.RetentionPolicy)
				errs = append(errs, err)
			} else if err := s.Store.CreateShard(sgfi.Database, sgfi.RetentionPolicy, sgfi.ShardGroup.Shards[0].ID, true); err != nil {
				// TODO(DSB): is Shards[0] always the right shard to create?  Yes for OSS, no for Enterprise
				decoratedErr := fmt.Errorf("failed to create shard %d for shard group %d covering %s to %s for database %s and retention policy %s: %w",
					sgfi.ShardGroup.Shards[0].ID,
					sgfi.ShardGroup.ID,
					sgfi.ShardGroup.StartTime,
					sgfi.ShardGroup.EndTime,
					sgfi.Database,
					sgfi.RetentionPolicy,
					err)
				errs = append(errs, decoratedErr)
			} else {
				s.Logger.Debug("Created shard",
					zap.String("database", sgfi.Database),
					zap.String("retention_policy", sgfi.RetentionPolicy),
					zap.Uint64("shard_group_id", sgfi.ShardGroup.ID),
					zap.Uint64("shard_id", sgfi.ShardGroup.Shards[0].ID),
					zap.Time("start_time", sgfi.ShardGroup.StartTime),
					zap.Time("end_time", sgfi.ShardGroup.EndTime))
			}
		}
		return errors.Join(errs...)
	}
}
