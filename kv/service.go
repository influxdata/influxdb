package kv

import (
	"context"
	"time"

	"github.com/benbjohnson/clock"
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
	log    *zap.Logger
	clock  clock.Clock
	Config ServiceConfig

	IDGenerator influxdb.IDGenerator

	// special ID generator that never returns bytes with backslash,
	// comma, or space. Used to support very specific encoding of org &
	// bucket into the old measurement in storage.
	OrgBucketIDs influxdb.IDGenerator

	TokenGenerator influxdb.TokenGenerator
	// TODO(desa:ariel): this should not be embedded
	influxdb.TimeGenerator
	Hash Crypt

	checkStore    *IndexStore
	endpointStore *IndexStore
	variableStore *IndexStore
}

// NewService returns an instance of a Service.
func NewService(log *zap.Logger, kv Store, configs ...ServiceConfig) *Service {
	s := &Service{
		log:         log,
		IDGenerator: snowflake.NewIDGenerator(),
		// Seed the random number generator with the current time
		OrgBucketIDs:   rand.NewOrgBucketID(time.Now().UnixNano()),
		TokenGenerator: rand.NewTokenGenerator(64),
		Hash:           &Bcrypt{},
		kv:             kv,
		TimeGenerator:  influxdb.RealTimeGenerator{},
		checkStore:     newCheckStore(),
		endpointStore:  newEndpointStore(),
		variableStore:  newVariableStore(),
	}

	if len(configs) > 0 {
		s.Config = configs[0]
	} else {
		s.Config.SessionLength = influxdb.DefaultSessionLength
	}

	s.clock = s.Config.Clock
	if s.clock == nil {
		s.clock = clock.New()
	}

	return s
}

// ServiceConfig allows us to configure Services
type ServiceConfig struct {
	SessionLength time.Duration
	Clock         clock.Clock
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

		if err := s.variableStore.Init(ctx, tx); err != nil {
			return err
		}

		if err := s.initializeVariablesOrgIndex(tx); err != nil {
			return err
		}

		if err := s.checkStore.Init(ctx, tx); err != nil {
			return err

		}

		if err := s.initializeNotificationRule(ctx, tx); err != nil {
			return err
		}

		if err := s.endpointStore.Init(ctx, tx); err != nil {
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

// WithSpecialOrgBucketIDs sets the generator for the org
// and bucket ids.
//
// Should only be used in tests for mocking.
func (s *Service) WithSpecialOrgBucketIDs(gen influxdb.IDGenerator) {
	s.OrgBucketIDs = gen
}
