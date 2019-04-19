package kv

import (
	"context"

	"go.uber.org/zap"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/rand"
	"github.com/influxdata/influxdb/snowflake"
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
	influxdb.TimeGenerator
	Hash Crypt
}

// NewService returns an instance of a Service.
func NewService(kv Store) *Service {
	return &Service{
		Logger:         zap.NewNop(),
		IDGenerator:    snowflake.NewIDGenerator(),
		TokenGenerator: rand.NewTokenGenerator(64),
		Hash:           &Bcrypt{},
		kv:             kv,
		TimeGenerator:  influxdb.RealTimeGenerator{},
	}
}

// Initialize creates Buckets needed.
func (s *Service) Initialize(ctx context.Context) error {
	return s.kv.Update(ctx, func(tx Tx) error {
		if err := s.initializeAuths(ctx, tx); err != nil {
			return err
		}

		if err := s.initializeDocuments(ctx, tx); err != nil {
			return err
		}

		if err := s.initializeBuckets(ctx, tx); err != nil {
			return err
		}

		if err := s.initializeDashboards(ctx, tx); err != nil {
			return err
		}

		if err := s.initializeKVLog(ctx, tx); err != nil {
			return err
		}

		if err := s.initializeLabels(ctx, tx); err != nil {
			return err
		}

		if err := s.initializeOnboarding(ctx, tx); err != nil {
			return err
		}

		if err := s.initializeOrgs(ctx, tx); err != nil {
			return err
		}

		if err := s.initializeTasks(ctx, tx); err != nil {
			return err
		}

		if err := s.initializePasswords(ctx, tx); err != nil {
			return err
		}

		if err := s.initializeScraperTargets(ctx, tx); err != nil {
			return err
		}

		if err := s.initializeSecrets(ctx, tx); err != nil {
			return err
		}

		if err := s.initializeSessions(ctx, tx); err != nil {
			return err
		}

		if err := s.initializeSources(ctx, tx); err != nil {
			return err
		}

		if err := s.initializeTelegraf(ctx, tx); err != nil {
			return err
		}

		if err := s.initializeURMs(ctx, tx); err != nil {
			return err
		}

		if err := s.initializeVariables(ctx, tx); err != nil {
			return err
		}

		return s.initializeUsers(ctx, tx)
	})
}

// WithStore sets kv store for the service.
// Should only be used in tests for mocking.
func (s *Service) WithStore(store Store) {
	s.kv = store
}
