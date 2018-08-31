package storage

import (
	"time"

	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/tsdb"
	"go.uber.org/zap"
)

type MetaClient interface {
	Database(name string) *meta.DatabaseInfo
	ShardGroupsByTimeRange(database, policy string, min, max time.Time) (a []meta.ShardGroupInfo, err error)
}

// Service manages the listener and handler for an HTTP endpoint.
type Service struct {
	addr           string
	grpc           *grpcServer
	loggingEnabled bool
	logger         *zap.Logger

	Store      *localStore
	TSDBStore  *tsdb.Store
	MetaClient MetaClient
}

// NewService returns a new instance of Service.
func NewService(c Config) *Service {
	s := &Service{
		addr:           c.BindAddress,
		loggingEnabled: c.LogEnabled,
		logger:         zap.NewNop(),
	}

	return s
}

// WithLogger sets the logger for the service.
func (s *Service) WithLogger(log *zap.Logger) {
	s.logger = log.With(zap.String("service", "storage"))
}

// Open starts the service.
func (s *Service) Open() error {
	s.logger.Info("Starting storage service")

	store := NewStore(s.TSDBStore, s.MetaClient)
	s.WithLogger(s.logger)

	grpc := &grpcServer{
		addr:           s.addr,
		loggingEnabled: s.loggingEnabled,
		logger:         s.logger,
		store:          store,
	}
	if err := grpc.Open(); err != nil {
		return err
	}

	s.grpc = grpc

	return nil
}

func (s *Service) Close() error {
	if s.grpc != nil {
		s.grpc.Close()
		s.grpc = nil
	}

	return nil
}
