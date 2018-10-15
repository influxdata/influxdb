package inmem

import (
	"sync"

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

	TokenGenerator platform.TokenGenerator
	IDGenerator    platform.IDGenerator
}

// NewService creates an instance of a Service.
func NewService() *Service {
	return &Service{
		TokenGenerator: rand.NewTokenGenerator(64),
		IDGenerator:    snowflake.NewIDGenerator(),
	}
}
