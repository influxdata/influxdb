package telemetry

import (
	"context"
	"time"

	influxlogger "github.com/influxdata/influxdb/logger"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

// Reporter reports telemetry metrics to a prometheus push
// gateway every interval.
type Reporter struct {
	Pusher   *Pusher
	Logger   *zap.Logger
	Interval time.Duration
}

// NewReporter reports telemetry every 24 hours.
func NewReporter(g prometheus.Gatherer) *Reporter {
	return &Reporter{
		Pusher:   NewPusher(g),
		Logger:   zap.NewNop(),
		Interval: 24 * time.Hour,
	}
}

// Report starts periodic telemetry reporting each interval.
func (r *Reporter) Report(ctx context.Context) {
	logger := r.Logger.With(
		zap.String("service", "telemetry"),
		influxlogger.DurationLiteral("interval", r.Interval),
	)

	logger.Info("Starting")
	if err := r.Pusher.Push(ctx); err != nil {
		logger.Debug("failure reporting telemetry metrics", zap.Error(err))
	}

	ticker := time.NewTicker(r.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			logger.Debug("Reporting")
			if err := r.Pusher.Push(ctx); err != nil {
				logger.Debug("failure reporting telemetry metrics", zap.Error(err))
			}
		case <-ctx.Done():
			logger.Info("Stopping")
			return
		}
	}
}
