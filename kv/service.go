package kv

import (
	"context"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/rand"
	"github.com/influxdata/influxdb/v2/resource"
	"github.com/influxdata/influxdb/v2/resource/noop"
	"github.com/influxdata/influxdb/v2/snowflake"
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

	// FluxLanguageService is used for parsing flux.
	// If this is unset, operations that require parsing flux
	// will fail.
	FluxLanguageService influxdb.FluxLanguageService

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
		urmByUserIndex: NewIndex(URMByUserIndexMapping, WithIndexReadPathEnabled),
		disableAuthorizationsForMaxPermissions: func(context.Context) bool {
			return false
		},
	}

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
	s.FluxLanguageService = s.Config.FluxLanguageService

	return s
}

// ServiceConfig allows us to configure Services
type ServiceConfig struct {
	SessionLength       time.Duration
	Clock               clock.Clock
	FluxLanguageService influxdb.FluxLanguageService
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
