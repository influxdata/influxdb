package inmem

import (
	"context"
	"sync"

	platform2 "github.com/influxdata/influxdb/v2/kit/platform"

	platform "github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/rand"
	"github.com/influxdata/influxdb/v2/snowflake"
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
	sessionKV             sync.Map
	sourceKV              sync.Map

	TokenGenerator platform.TokenGenerator
	IDGenerator    platform2.IDGenerator
	platform.TimeGenerator
}

// NewService creates an instance of a Service.
func NewService() *Service {
	s := &Service{
		TokenGenerator: rand.NewTokenGenerator(64),
		IDGenerator:    snowflake.NewIDGenerator(),
		TimeGenerator:  platform.RealTimeGenerator{},
	}
	s.initializeSources(context.TODO())
	return s
}

// Flush removes all data from the in-memory store
func (s *Service) Flush() {
	s.flush(&s.authorizationKV)
	s.flush(&s.organizationKV)
	s.flush(&s.bucketKV)
	s.flush(&s.userKV)
	s.flush(&s.dashboardKV)
	s.flush(&s.viewKV)
	s.flush(&s.variableKV)
	s.flush(&s.dbrpMappingKV)
	s.flush(&s.userResourceMappingKV)
	s.flush(&s.labelKV)
	s.flush(&s.labelMappingKV)
	s.flush(&s.scraperTargetKV)
	s.flush(&s.telegrafConfigKV)
	s.flush(&s.onboardingKV)
	s.flush(&s.basicAuthKV)
	s.flush(&s.sessionKV)
	s.flush(&s.sourceKV)
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
