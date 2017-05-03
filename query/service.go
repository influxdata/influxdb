package query

import (
	"log"
	"os"
	"time"

	"github.com/influxdb/influxdb/influxql"
	"github.com/influxdb/influxdb/meta"
)

type Service struct {
	// The meta store for accessing cluster Metadata.
	MetaStore interface {
		Database(name string) (*meta.DatabaseInfo, error)
		User(name string) (*meta.UserInfo, error)
		AdminUserExists() (bool, error)
		Authenticate(username, password string) (*meta.UserInfo, error)
		RetentionPolicy(database, name string) (rpi *meta.RetentionPolicyInfo, err error)
		UserCount() (int, error)
	}

	remoteNodeTimeout time.Duration

	Logger *log.Logger
}

func NewService(c Config) *Service {
	return &Service{
		Logger: log.New(os.Stderr, "[query] ", log.LstdFlags),
	}
}

func (s *Service) Open() error {
	return nil
}

func (s *Service) Close() error {
	return nil
}

func (s *Service) ExecuteQuery(q *influxql.Query, db string, chunkSize int) (<-chan *influxql.Result, error) {
	return nil, nil
}

func (s *Service) SetLogger(l *log.Logger) {
	s.Logger = l
}
