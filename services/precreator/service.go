// Package precreator provides the shard precreation service.
package precreator // import "github.com/influxdata/influxdb/services/precreator"

import (
	"context"
	"sync"
	"time"

	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/services"
	"go.uber.org/zap"
)

// Service manages the shard precreation service.
type Service struct {
	checkInterval time.Duration
	advancePeriod time.Duration

	Logger *zap.Logger

	wg sync.WaitGroup

	MetaClient interface {
		PrecreateShardGroups(now, cutoff time.Time) error
	}

	// members used for testing
	ctx     context.Context
	cancel  context.CancelFunc
	errChan chan error
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

func (s *Service) Open() error {
	s.errChan = make(chan error)
	s.ctx, s.cancel = context.WithCancel(context.Background())
	go func() { s.errChan <- s.Run(s.ctx, nil) }()
	return nil
}

func (s *Service) Close() error {
	s.cancel()
	err, ok := <-s.errChan

	if !ok {
		return nil
	}

	close(s.errChan)
	return err
}

// Open starts the precreation service.
func (s *Service) Run(ctx context.Context, reg services.Registry) error {
	s.Logger.Info("Starting precreation service",
		logger.DurationLiteral("check_interval", s.checkInterval),
		logger.DurationLiteral("advance_period", s.advancePeriod))

	s.wg.Add(1)
	return s.runPrecreation(ctx)
}

// runPrecreation continually checks if resources need precreation.
func (s *Service) runPrecreation(ctx context.Context) error {
	defer s.wg.Done()
	for {
		select {
		case <-time.After(s.checkInterval):
			if err := s.precreate(time.Now().UTC()); err != nil {
				s.Logger.Info("Failed to precreate shards", zap.Error(err))
			}
		case <-ctx.Done():
			s.Logger.Info("Terminating precreation service")
			return nil
		}
	}
}

// precreate performs actual resource precreation.
func (s *Service) precreate(now time.Time) error {
	cutoff := now.Add(s.advancePeriod).UTC()
	return s.MetaClient.PrecreateShardGroups(now, cutoff)
}
