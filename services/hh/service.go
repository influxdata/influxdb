package hh

import (
	"log"
	"os"

	"github.com/influxdb/influxdb/tsdb"
)

type Service struct {
	Logger *log.Logger
	cfg    Config
}

// NewService returns a new instance of Service.
func NewService(c Config) *Service {
	s := &Service{
		cfg:    c,
		Logger: log.New(os.Stderr, "[handoff] ", log.LstdFlags),
	}
	return s
}

func (s *Service) WriteShard(shardID, ownerID uint64, points []tsdb.Point) error {
	return nil
}

func (s *Service) Open() error  { return nil }
func (s *Service) Close() error { return nil }
