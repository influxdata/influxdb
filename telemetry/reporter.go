package telemetry

import (
	"context"
	"time"

	influxlogger "github.com/influxdata/influxdb/v2/logger"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

// Reporter reports telemetry metrics to a prometheus push
// gateway every interval.
type Reporter struct {
	Pusher   *Pusher
	log      *zap.Logger
	Interval time.Duration
}

// NewReporter reports telemetry every 24 hours.
func NewReporter(log *zap.Logger, g prometheus.Gatherer) *Reporter {
	return &Reporter{
		Pusher:   NewPusher(g),
		log:      log,
		Interval: 24 * time.Hour,
	}
}

// Report starts periodic telemetry reporting each interval.
func (r *Reporter) Report(ctx context.Context) {
	logger := r.log.With(
		zap.String("service", "telemetry"),
		influxlogger.DurationLiteral("interval", r.Interval),
	)

	logger.Info("Starting")
	if err := r.Pusher.Push(ctx); err != nil {
		logger.Debug("Failure reporting telemetry metrics", zap.Error(err))
	}

	ticker := time.NewTicker(r.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			logger.Debug("Reporting")
			if err := r.Pusher.Push(ctx); err != nil {
				logger.Debug("Failure reporting telemetry metrics", zap.Error(err))
			}
		case <-ctx.Done():
			logger.Info("Stopping")
			return
		}
	}
}
