package inmem

import (
	"sync"
	"time"

	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/rand"
	"github.com/influxdata/influxdb/snowflake"
)

// OpPrefix is the op prefix.
const OpPrefix = "inmem/"

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
	labelKV               sync.Map
	labelMappingKV        sync.Map
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

// Flush removes all data from the in-memory store
func (s *Service) Flush() {
	s.flush(&s.authorizationKV)
	s.flush(&s.organizationKV)
	s.flush(&s.bucketKV)
	s.flush(&s.userKV)
	s.flush(&s.dashboardKV)
	s.flush(&s.viewKV)
	s.flush(&s.macroKV)
	s.flush(&s.dbrpMappingKV)
	s.flush(&s.userResourceMappingKV)
	s.flush(&s.labelKV)
	s.flush(&s.labelMappingKV)
	s.flush(&s.scraperTargetKV)
	s.flush(&s.telegrafConfigKV)
	s.flush(&s.onboardingKV)
	s.flush(&s.basicAuthKV)
}

func (s *Service) flush(m *sync.Map) {
	keys := []interface{}{}
	f := func(key, value interface{}) bool {
		keys = append(keys, key)
		return true
	}

	m.Range(f)

	for _, k := range keys {
		m.Delete(k)
	}

}
