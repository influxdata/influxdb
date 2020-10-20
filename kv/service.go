package kv

import (
	"github.com/benbjohnson/clock"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/rand"
	"github.com/influxdata/influxdb/v2/resource"
	"github.com/influxdata/influxdb/v2/resource/noop"
	"github.com/influxdata/influxdb/v2/snowflake"
	"go.uber.org/zap"
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

	TokenGenerator influxdb.TokenGenerator
	// TODO(desa:ariel): this should not be embedded
	influxdb.TimeGenerator

	orgs influxdb.OrganizationService

	variableStore *IndexStore
}

// NewService returns an instance of a Service.
func NewService(log *zap.Logger, kv Store, orgs influxdb.OrganizationService, configs ...ServiceConfig) *Service {
	s := &Service{
		log:            log,
		IDGenerator:    snowflake.NewIDGenerator(),
		TokenGenerator: rand.NewTokenGenerator(64),
		kv:             kv,
		orgs:           orgs,
		audit:          noop.ResourceLogger{},
		TimeGenerator:  influxdb.RealTimeGenerator{},
		variableStore:  newVariableStore(),
	}

	if len(configs) > 0 {
		s.Config = configs[0]
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
