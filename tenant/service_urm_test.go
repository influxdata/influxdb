package tenant_test

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/tenant"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
)

func TestBoltUserResourceMappingService(t *testing.T) {
	influxdbtesting.UserResourceMappingService(initBoltUserResourceMappingService, t)
}

func initBoltUserResourceMappingService(f influxdbtesting.UserResourceFields, t *testing.T) (influxdb.UserResourceMappingService, func()) {
	s, closeBolt, err := NewTestBoltStore(t)
	if err != nil {
		t.Fatalf("failed to create new kv store: %v", err)
	}

	svc, closeSvc := initUserResourceMappingService(s, f, t)
	return svc, func() {
		closeSvc()
		closeBolt()
	}
}

func initUserResourceMappingService(s kv.Store, f influxdbtesting.UserResourceFields, t *testing.T) (influxdb.UserResourceMappingService, func()) {
	storage, err := tenant.NewStore(s)
	if err != nil {
		t.Fatal(err)
	}
	svc := tenant.NewService(storage)

	// Create resources before mappings.

	for _, u := range f.Users {
		if err := svc.CreateUser(context.Background(), u); err != nil {
			t.Fatalf("error populating users: %v", err)
		}
	}

	for _, o := range f.Organizations {
		if err := svc.CreateOrganization(context.Background(), o); err != nil {
			t.Fatalf("failed to populate organizations: %s", err)
		}
	}

	for _, b := range f.Buckets {
		if err := svc.CreateBucket(context.Background(), b); err != nil {
			t.Fatalf("failed to populate buckets: %s", err)
		}
	}

	// Now create mappings.

	for _, m := range f.UserResourceMappings {
		if err := svc.CreateUserResourceMapping(context.Background(), m); err != nil {
			t.Fatalf("failed to populate mappings: %v", err)
		}
	}

	return svc, func() {
		for _, u := range f.Users {
			if err := svc.DeleteUser(context.Background(), u.ID); err != nil {
				t.Logf("error removing users: %v", err)
			}
		}

		for _, m := range f.UserResourceMappings {
			if err := svc.DeleteUserResourceMapping(context.Background(), m.ResourceID, m.UserID); err != nil {
				t.Logf("failed to remove user resource mapping: %v", err)
			}
		}

		for _, o := range f.Organizations {
			if err := svc.DeleteOrganization(context.Background(), o.ID); err != nil {
				t.Logf("failed to remove organization: %v", err)
			}
		}
	}
}
