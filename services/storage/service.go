package storage

import (
	"time"

	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/uber-go/zap"
)

// Service manages the listener and handler for an HTTP endpoint.
type Service struct {
	addr  string
	addr2 string
	addr3 string

	yarpc *yarpcServer

	Store      *Store
	TSDBStore  *tsdb.Store
	MetaClient interface {
		Database(name string) *meta.DatabaseInfo
		ShardGroupsByTimeRange(database, policy string, min, max time.Time) (a []meta.ShardGroupInfo, err error)
	}
	Logger zap.Logger
}

// NewService returns a new instance of Service.
func NewService(c Config) *Service {
	s := &Service{
		addr:   c.BindAddress,
		Logger: zap.New(zap.NullEncoder()),
	}

	return s
}

// Open starts the service.
func (s *Service) Open() error {
	s.Logger.Info("Starting storage service")

	store := NewStore()
	store.TSDBStore = s.TSDBStore
	store.MetaClient = s.MetaClient
	store.Logger = s.Logger

	yarpc := newYARPCServer(s.addr)
	yarpc.Logger = s.Logger
	yarpc.Store = store
	if err := yarpc.Open(); err != nil {
		return err
	}

	s.yarpc = yarpc

	return nil
}

func (s *Service) Close() error {
	if s.yarpc != nil {
		s.yarpc.Close()
	}

	return nil
}

// WithLogger sets the logger for the service.
func (s *Service) WithLogger(log zap.Logger) {
	s.Logger = log.With(zap.String("service", "storage"))
}
