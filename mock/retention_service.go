package mock

import (
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

type RetentionService struct {
	WithLoggerFn           func(l *zap.Logger)
	OpenFn                 func() error
	CloseFn                func() error
	PrometheusCollectorsFn func() []prometheus.Collector
}

func NewRetentionService() *RetentionService {
	return &RetentionService{
		WithLoggerFn:           func(_ *zap.Logger) {},
		OpenFn:                 func() error { return nil },
		CloseFn:                func() error { return nil },
		PrometheusCollectorsFn: func() []prometheus.Collector { return nil },
	}
}

func (s *RetentionService) WithLogger(log *zap.Logger) {
	s.WithLoggerFn(log)
}

func (s *RetentionService) Open() error {
	return s.OpenFn()
}

func (s *RetentionService) Close() error {
	return s.CloseFn()
}

func (s *RetentionService) PrometheusCollectors() []prometheus.Collector {
	return s.PrometheusCollectorsFn()
}
