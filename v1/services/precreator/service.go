// Package precreator provides the shard precreation service.
package precreator // import "github.com/influxdata/influxdb/v2/v1/services/precreator"

import (
	"context"
	"sync"
	"time"

	"github.com/influxdata/influxdb/v2/logger"
	"go.uber.org/zap"
)

// Service manages the shard precreation service.
type Service struct {
	checkInterval time.Duration
	advancePeriod time.Duration

	Logger *zap.Logger

	cancel context.CancelFunc
	wg     sync.WaitGroup

	MetaClient interface {
		PrecreateShardGroups(now, cutoff time.Time) error
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
func (s *Service) Open(ctx context.Context) error {
	if s.cancel != nil {
		return nil
	}

	s.Logger.Info("Starting precreation service",
		logger.DurationLiteral("check_interval", s.checkInterval),
		logger.DurationLiteral("advance_period", s.advancePeriod))

	ctx, s.cancel = context.WithCancel(ctx)

	s.wg.Add(1)
	go s.runPrecreation(ctx)
	return nil
}

// Close stops the precreation service.
func (s *Service) Close() error {
	if s.cancel == nil {
		return nil
	}

	s.cancel()
	s.wg.Wait()
	s.cancel = nil

	return nil
}

// runPrecreation continually checks if resources need precreation.
func (s *Service) runPrecreation(ctx context.Context) {
	defer s.wg.Done()

	for {
		select {
		case <-time.After(s.checkInterval):
			if err := s.precreate(time.Now().UTC()); err != nil {
				s.Logger.Info("Failed to precreate shards", zap.Error(err))
			}
		case <-ctx.Done():
			s.Logger.Info("Terminating precreation service")
			return
		}
	}
}

// precreate performs actual resource precreation.
func (s *Service) precreate(now time.Time) error {
	cutoff := now.Add(s.advancePeriod).UTC()
	return s.MetaClient.PrecreateShardGroups(now, cutoff)
}
