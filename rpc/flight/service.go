package flight

import (
	"context"
	"net"
	"sync"

	"github.com/influxdata/influxdb/query/control"
	"github.com/influxdata/influxdb/storage/reads"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

type Service struct {
	log   *zap.Logger
	store reads.Store
	query *control.Controller
	grpc  *storeGRPCServer

	mu       sync.Mutex
	_closing chan struct{}
}

func NewService(log *zap.Logger, store reads.Store, query *control.Controller) *Service {
	s := &Service{
		log:   log,
		store: store,
		query: query,
		grpc: &storeGRPCServer{
			loggingEnabled: true,
			store:          store,
			query:          query,
			logger:         log,
		},
	}

	return s
}

func (s *Service) Serve(grpcListener net.Listener) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s._closing != nil {
		return nil // Already open.
	}

	s._closing = make(chan struct{})

	return s.grpc.Serve(grpcListener)
}

func (s *Service) Shutdown(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s._closing == nil {
		return
	}

	close(s._closing)
	s._closing = nil

	s.grpc.Shutdown(ctx)
}

// PrometheusCollectors returns the metrics for the gRPC server.
func (s *Service) PrometheusCollectors() []prometheus.Collector {
	return s.grpc.PrometheusCollectors()
}
