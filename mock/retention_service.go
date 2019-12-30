package mock

import (
	"github.com/prometheus/client_golang/prometheus"
)

type RetentionService struct {
	OpenFn                 func() error
	CloseFn                func() error
	PrometheusCollectorsFn func() []prometheus.Collector
}

func NewRetentionService() *RetentionService {
	return &RetentionService{
		OpenFn:                 func() error { return nil },
		CloseFn:                func() error { return nil },
		PrometheusCollectorsFn: func() []prometheus.Collector { return nil },
	}
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
