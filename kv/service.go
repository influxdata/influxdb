package kv

import (
	"context"
	"time"

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
	Hash           Crypt

	time func() time.Time
}

// NewService returns an instance of a Service.
func NewService(kv Store) *Service {
	return &Service{
		Logger:         zap.NewNop(),
		IDGenerator:    snowflake.NewIDGenerator(),
		TokenGenerator: rand.NewTokenGenerator(64),
		Hash:           &Bcrypt{},
		kv:             kv,
		time:           time.Now,
	}
}

// Initialize creates Buckets needed.
func (s *Service) Initialize(ctx context.Context) error {
	return s.kv.Update(func(tx Tx) error {
		if err := s.initializeSources(ctx, tx); err != nil {
			return err
		}

		if err := s.initializeMacros(ctx, tx); err != nil {
			return err
		}

		if err := s.initializeUsers(ctx, tx); err != nil {
			return err
		}

		if err := s.initializeOrgs(ctx, tx); err != nil {
			return err
		}

		if err := s.initializeBuckets(ctx, tx); err != nil {
			return err
		}

		if err := s.initializeBuckets(ctx, tx); err != nil {
			return err
		}

		if err := s.initializeKVLog(ctx, tx); err != nil {
			return err
		}

		if err := s.initializeURMs(ctx, tx); err != nil {
			return err
		}

		if err := s.initializeAuths(ctx, tx); err != nil {
			return err
		}

		if err := s.initializeDashboards(ctx, tx); err != nil {
			return err
		}

		if err := s.initializeLabels(ctx, tx); err != nil {
			return err
		}

		if err := s.initializePasswords(ctx, tx); err != nil {
			return err
		}

		return nil
	})
}

// WithTime sets the function for computing the current time. Used for updating meta data
// about objects stored. Should only be used in tests for mocking.
func (s *Service) WithTime(fn func() time.Time) {
	s.time = fn
}

// WithStore sets kv store for the service.
// Should only be used in tests for mocking.
func (s *Service) WithStore(store Store) {
	s.kv = store
}
