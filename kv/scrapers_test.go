package kv_test

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/kv"
	influxdbtesting "github.com/influxdata/influxdb/testing"
)

func TestBoltScraperTargetStoreService(t *testing.T) {
	influxdbtesting.ScraperService(initBoltTargetService, t)
}

func TestInmemScraperTargetStoreService(t *testing.T) {
	influxdbtesting.ScraperService(initInmemTargetService, t)
}

func initBoltTargetService(f influxdbtesting.TargetFields, t *testing.T) (influxdb.ScraperTargetStoreService, string, func()) {
	s, closeFn, err := NewTestBoltStore()
	if err != nil {
		t.Fatalf("failed to create new kv store: %v", err)
	}

	svc, op, closeSvc := initScraperTargetStoreService(s, f, t)
	return svc, op, func() {
		closeSvc()
		closeFn()
	}
}

func initInmemTargetService(f influxdbtesting.TargetFields, t *testing.T) (influxdb.ScraperTargetStoreService, string, func()) {
	s, closeFn, err := NewTestInmemStore()
	if err != nil {
		t.Fatalf("failed to create new kv store: %v", err)
	}

	svc, op, closeSvc := initScraperTargetStoreService(s, f, t)
	return svc, op, func() {
		closeSvc()
		closeFn()
	}
}

func initScraperTargetStoreService(s kv.Store, f influxdbtesting.TargetFields, t *testing.T) (influxdb.ScraperTargetStoreService, string, func()) {
	svc := kv.NewService(s)
	svc.IDGenerator = f.IDGenerator

	ctx := context.Background()
	if err := svc.Initialize(ctx); err != nil {
		t.Fatalf("error initializing user service: %v", err)
	}

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
