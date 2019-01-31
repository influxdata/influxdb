package inmem

import (
	"sync"
	"time"

	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/rand"
	"github.com/influxdata/influxdb/snowflake"
	"github.com/prometheus/client_golang/prometheus"
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
	variableKV            sync.Map
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

// ID returns a unique ID for this service.
func (s *Service) ID() platform.ID {
	return s.IDGenerator.ID()
}

// PrometheusCollectors returns all collectors for the inmem service.
func (s *Service) PrometheusCollectors() []prometheus.Collector {
	return nil
}

// Describe returns all descriptions of the collector.
func (s *Service) Describe(ch chan<- *prometheus.Desc) {}

// Collect returns the current state of all metrics of the collector.
func (s *Service) Collect(ch chan<- prometheus.Metric) {}
