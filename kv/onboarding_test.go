package kv_test

import (
	"context"
	"testing"

	influxdb "github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kv"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
	"go.uber.org/zap/zaptest"
)

func TestBoltOnboardingService(t *testing.T) {
	influxdbtesting.OnboardInitialUser(initBoltOnboardingService, t)
}

func initBoltOnboardingService(f influxdbtesting.OnboardingFields, t *testing.T) (influxdb.OnboardingService, func()) {
	s, closeStore, err := NewTestBoltStore(t)
	if err != nil {
		t.Fatalf("failed to create new bolt kv store: %v", err)
	}

	svc, closeSvc := initOnboardingService(s, f, t)
	return svc, func() {
		closeSvc()
		closeStore()
	}
}

func initOnboardingService(s kv.SchemaStore, f influxdbtesting.OnboardingFields, t *testing.T) (influxdb.OnboardingService, func()) {
	ctx := context.Background()
	svc := kv.NewService(zaptest.NewLogger(t), s)
	svc.IDGenerator = f.IDGenerator
	svc.OrgBucketIDs = f.IDGenerator
	svc.TokenGenerator = f.TokenGenerator
	svc.TimeGenerator = f.TimeGenerator
	if f.TimeGenerator == nil {
		svc.TimeGenerator = influxdb.RealTimeGenerator{}
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
