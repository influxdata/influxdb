package inmem

import (
	"sync"
	"time"

	"github.com/influxdata/platform"
	"github.com/influxdata/platform/rand"
	"github.com/influxdata/platform/snowflake"
)

// Service implements various top level services.
type Service struct {
	authorizationKV       sync.Map
	organizationKV        sync.Map
	bucketKV              sync.Map
	userKV                sync.Map
	dashboardKV           sync.Map
	viewKV                sync.Map
	macroKV               sync.Map
	dbrpMappingKV         sync.Map
	userResourceMappingKV sync.Map
	scraperTargetKV       sync.Map
	telegrafConfigKV      sync.Map
	onboardingKV          sync.Map
	basicAuthKV           sync.Map

	TokenGenerator platform.TokenGenerator
	IDGenerator    platform.IDGenerator
	time           func() time.Time
}

// NewService creates an instance of a Service.
func NewService() *Service {
	return &Service{
		TokenGenerator: rand.NewTokenGenerator(64),
		IDGenerator:    snowflake.NewIDGenerator(),
		time:           time.Now,
	}
}

// WithTime sets the function for computing the current time. Used for updating meta data
// about objects stored. Should only be used in tests for mocking.
func (s *Service) WithTime(fn func() time.Time) {
	s.time = fn
}
