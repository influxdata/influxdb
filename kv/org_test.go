package kv_test

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/inmem"
	"github.com/influxdata/influxdb/kv"
	influxdbtesting "github.com/influxdata/influxdb/testing"
)

func TestBoltOrganizationService(t *testing.T) {
	influxdbtesting.OrganizationService(initBoltOrganizationService, t)
}

func TestInmemOrganizationService(t *testing.T) {
	influxdbtesting.OrganizationService(initInmemOrganizationService, t)
}

func initBoltOrganizationService(f influxdbtesting.OrganizationFields, t *testing.T) (influxdb.OrganizationService, string, func()) {
	s, closeBolt, err := NewTestBoltStore()
	if err != nil {
		t.Fatalf("failed to create new kv store: %v", err)
	}

	svc, op, closeSvc := initOrganizationService(s, f, t)
	return svc, op, func() {
		closeSvc()
		closeBolt()
	}
}

func initInmemOrganizationService(f influxdbtesting.OrganizationFields, t *testing.T) (influxdb.OrganizationService, string, func()) {
	s, closeBolt, err := NewTestInmemStore()
	if err != nil {
		t.Fatalf("failed to create new kv store: %v", err)
	}

	svc, op, closeSvc := initOrganizationService(s, f, t)
	return svc, op, func() {
		closeSvc()
		closeBolt()
	}
}

func initOrganizationService(s kv.Store, f influxdbtesting.OrganizationFields, t *testing.T) (influxdb.OrganizationService, string, func()) {
	svc := kv.NewService(s)
	svc.IDGenerator = f.IDGenerator
	svc.TimeGenerator = f.TimeGenerator
	if f.TimeGenerator == nil {
		svc.TimeGenerator = influxdb.RealTimeGenerator{}
	}

	ctx := context.Background()
	if err := svc.Initialize(ctx); err != nil {
		t.Fatalf("error initializing organization service: %v", err)
	}

	for _, u := range f.Organizations {
		if err := svc.PutOrganization(ctx, u); err != nil {
			t.Fatalf("failed to populate organizations")
		}
	}

	return svc, kv.OpPrefix, func() {
		for _, u := range f.Organizations {
			if err := svc.DeleteOrganization(ctx, u.ID); err != nil {
				t.Logf("failed to remove organizations: %v", err)
			}
		}
	}
}

func TestService_CreateOrganization(t *testing.T) {
	t.Run("InvalidOrgID", func(t *testing.T) {
		svc := kv.NewService(inmem.NewKVStore())
		svc.IsValidOrgBucketID = func(id influxdb.ID) bool { return false }
		if err := svc.CreateOrganization(context.Background(), &influxdb.Organization{Name: "ORG"}); err == nil || err.Error() != `unable to generate valid org id` {
			t.Fatalf("unexpected error: %#v", err)
		}
	})
}
