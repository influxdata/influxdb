package kv_test

import (
	"context"
	"testing"

	influxdb "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/kv"
	influxdbtesting "github.com/influxdata/influxdb/testing"
)

func TestBoltOnboardingService(t *testing.T) {
	influxdbtesting.Generate(initBoltOnboardingService, t)
}

func TestInmemOnboardingService(t *testing.T) {
	influxdbtesting.Generate(initInmemOnboardingService, t)
}

func initBoltOnboardingService(f influxdbtesting.OnboardingFields, t *testing.T) (influxdb.OnboardingService, func()) {
	s, closeStore, err := NewTestBoltStore()
	if err != nil {
		t.Fatalf("failed to create new bolt kv store: %v", err)
	}

	svc, closeSvc := initOnboardingService(s, f, t)
	return svc, func() {
		closeSvc()
		closeStore()
	}
}

func initInmemOnboardingService(f influxdbtesting.OnboardingFields, t *testing.T) (influxdb.OnboardingService, func()) {
	s, closeStore, err := NewTestInmemStore()
	if err != nil {
		t.Fatalf("failed to create new inmem kv store: %v", err)
	}

	svc, closeSvc := initOnboardingService(s, f, t)
	return svc, func() {
		closeSvc()
		closeStore()
	}
}

func initOnboardingService(s kv.Store, f influxdbtesting.OnboardingFields, t *testing.T) (influxdb.OnboardingService, func()) {
	svc := kv.NewService(s)
	svc.IDGenerator = f.IDGenerator
	svc.TokenGenerator = f.TokenGenerator
	ctx := context.Background()
	if err := svc.Initialize(ctx); err != nil {
		t.Fatalf("unable to initialize kv store: %v", err)
	}

	t.Logf("Onboarding: %v", f.IsOnboarding)
	if err := svc.PutOnboardingStatus(ctx, !f.IsOnboarding); err != nil {
		t.Fatalf("failed to set new onboarding finished: %v", err)
	}

	return svc, func() {
		if err := svc.PutOnboardingStatus(ctx, false); err != nil {
			t.Logf("failed to remove onboarding finished: %v", err)
		}
	}
}
