package storage

import (
	"time"

	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/tsdb"
	"go.uber.org/zap"
)

type StorageMetaClient interface {
	Database(name string) *meta.DatabaseInfo
	ShardGroupsByTimeRange(database, policy string, min, max time.Time) (a []meta.ShardGroupInfo, err error)
}

// Service manages the listener and handler for an HTTP endpoint.
type Service struct {
	addr           string
	yarpc          *yarpcServer
	loggingEnabled bool
	logger         *zap.Logger

	Store      *Store
	TSDBStore  *tsdb.Store
	MetaClient StorageMetaClient
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

	store := NewStore()
	store.TSDBStore = s.TSDBStore
	store.MetaClient = s.MetaClient
	store.Logger = s.logger

	yarpc := &yarpcServer{
		addr:           s.addr,
		loggingEnabled: s.loggingEnabled,
		logger:         s.logger,
		store:          store,
	}
	if err := yarpc.Open(); err != nil {
		return err
	}

	s.yarpc = yarpc

	return nil
}

func (s *Service) Close() error {
	if s.yarpc != nil {
		s.yarpc.Close()
		s.yarpc = nil
	}

	return nil
}
