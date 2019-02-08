package kv

import (
	"context"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/rand"
	"github.com/influxdata/influxdb/snowflake"
	"go.uber.org/zap"
)

var (
	_ influxdb.UserService = (*Service)(nil)
)

// OpPrefix is the prefix for kv errors.
const OpPrefix = "kv/"

// Service is the struct that influxdb services are implemented on.
type Service struct {
	kv     Store
	Logger *zap.Logger

	IDGenerator    influxdb.IDGenerator
	TokenGenerator influxdb.TokenGenerator
}

// NewService returns an instance of a Service.
func NewService(kv Store) *Service {
	return &Service{
		Logger:         zap.NewNop(),
		IDGenerator:    snowflake.NewIDGenerator(),
		TokenGenerator: rand.NewTokenGenerator(64),
		kv:             kv,
	}
}

// Initialize creates Buckets needed.
func (s *Service) Initialize(ctx context.Context) error {
	return s.kv.Update(func(tx Tx) error {
		if err := s.initializeUsers(ctx, tx); err != nil {
			return err
		}

		if err := s.initializeOrgs(ctx, tx); err != nil {
			return err
		}

		if err := s.initializeBuckets(ctx, tx); err != nil {
			return err
		}

		if err := s.initializeKVLog(ctx, tx); err != nil {
			return err
		}

		return nil
	})
}
