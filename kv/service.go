package kv

import (
	"context"
	"encoding/json"
	"time"

	"github.com/influxdata/influxdb/resource/noop"

	"github.com/benbjohnson/clock"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/rand"
	"github.com/influxdata/influxdb/resource"
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
	kv          Store
	log         *zap.Logger
	clock       clock.Clock
	Config      ServiceConfig
	audit       resource.Logger
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

	Migrator *Migrator

	urmByUserIndex *Index

	disableAuthorizationsForMaxPermissions func(context.Context) bool
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
		audit:          noop.ResourceLogger{},
		TimeGenerator:  influxdb.RealTimeGenerator{},
		checkStore:     newCheckStore(),
		endpointStore:  newEndpointStore(),
		variableStore:  newVariableStore(),
		Migrator:       NewMigrator(log),
		urmByUserIndex: NewIndex(NewIndexMapping(
			urmBucket,
			urmByUserIndexBucket,
			func(v []byte) ([]byte, error) {
				var urm influxdb.UserResourceMapping
				if err := json.Unmarshal(v, &urm); err != nil {
					return nil, err
				}

				id, _ := urm.UserID.Encode()
				return id, nil
			},
		)),
		disableAuthorizationsForMaxPermissions: func(context.Context) bool {
			return false
		},
	}

	// kv service migrations
	s.Migrator.AddMigrations(
		// initial migration is the state of the world when
		// the migrator was introduced.
		NewAnonymousMigration(
			"initial migration",
			s.initializeAll,
			// down is a noop
			func(context.Context, Store) error {
				return nil
			},
		),
		// add index user resource mappings by user id
		s.urmByUserIndex.Migration(),
		// and new migrations below here (and move this comment down):
	)

	if len(configs) > 0 {
		s.Config = configs[0]
	}

	if s.Config.SessionLength == 0 {
		s.Config.SessionLength = influxdb.DefaultSessionLength
	}

	s.clock = s.Config.Clock
	if s.clock == nil {
		s.clock = clock.New()
	}

	if s.Config.URMByUserIndexReadPathEnabled {
		WithIndexReadPathEnabled(s.urmByUserIndex)
	}

	return s
}

// ServiceConfig allows us to configure Services
type ServiceConfig struct {
	SessionLength                 time.Duration
	Clock                         clock.Clock
	URMByUserIndexReadPathEnabled bool
}

// AutoMigrationStore is a Store which also describes whether or not
// migrations can be applied automatically.
// Given the AutoMigrate method is defined and it returns a non-nil kv.Store
// implementation, then it will automatically invoke migrator.Up(store)
// on the returned kv.Store during Service.Initialize(...).
type AutoMigrationStore interface {
	Store
	AutoMigrate() Store
}

// Initialize creates Buckets needed.
func (s *Service) Initialize(ctx context.Context) error {
	if err := s.Migrator.Initialize(ctx, s.kv); err != nil {
		return err
	}

	// if store implements auto migrate and the resulting Store from
	// AutoMigrate() is non-nil, apply migrator.Up() to the resulting store.
	if store, ok := s.kv.(AutoMigrationStore); ok {
		if migrateStore := store.AutoMigrate(); migrateStore != nil {
			return s.Migrator.Up(ctx, migrateStore)
		}
	}

	return nil
}

func (s *Service) initializeAll(ctx context.Context, store Store) error {
	// please do not initialize anymore buckets here
	// add them as a new migration to the list of migrations
	// defined in NewService.
	if err := store.Update(ctx, func(tx Tx) error {
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
	}); err != nil {
		return err
	}

	return nil
}

// WithResourceLogger sets the resource audit logger for the service.
func (s *Service) WithResourceLogger(audit resource.Logger) {
	s.audit = audit
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

// WithMaxPermissionFunc sets the useAuthorizationsForMaxPermissions function
// which can trigger whether or not max permissions uses the users authorizations
// to derive maximum permissions.
func (s *Service) WithMaxPermissionFunc(fn func(context.Context) bool) {
	s.disableAuthorizationsForMaxPermissions = fn
}
