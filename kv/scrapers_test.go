package kv_test

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kv"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
	"go.uber.org/zap/zaptest"
)

func TestBoltScraperTargetStoreService(t *testing.T) {
	influxdbtesting.ScraperService(initBoltTargetService, t)
}

func initBoltTargetService(f influxdbtesting.TargetFields, t *testing.T) (influxdb.ScraperTargetStoreService, string, func()) {
	s, closeFn, err := NewTestBoltStore(t)
	if err != nil {
		t.Fatalf("failed to create new kv store: %v", err)
	}

	svc, op, closeSvc := initScraperTargetStoreService(s, f, t)
	return svc, op, func() {
		closeSvc()
		closeFn()
	}
}

func initScraperTargetStoreService(s kv.SchemaStore, f influxdbtesting.TargetFields, t *testing.T) (influxdb.ScraperTargetStoreService, string, func()) {
	ctx := context.Background()
	svc := kv.NewService(zaptest.NewLogger(t), s)
	svc.IDGenerator = f.IDGenerator

	for _, target := range f.Targets {
		if err := svc.PutTarget(ctx, target); err != nil {
			t.Fatalf("failed to populate targets: %v", err)
		}
	}

	for _, m := range f.UserResourceMappings {
		if err := svc.CreateUserResourceMapping(ctx, m); err != nil {
			t.Fatalf("failed to populate user resource mapping")
		}
	}

	for _, o := range f.Organizations {
		if err := svc.PutOrganization(ctx, o); err != nil {
			t.Fatalf("failed to populate organization")
		}
	}

	return svc, kv.OpPrefix, func() {
		for _, target := range f.Targets {
			if err := svc.RemoveTarget(ctx, target.ID); err != nil {
				t.Logf("failed to remove targets: %v", err)
			}
		}
		for _, o := range f.Organizations {
			if err := svc.DeleteOrganization(ctx, o.ID); err != nil {
				t.Logf("failed to remove orgs: %v", err)
			}
		}
	}
}
