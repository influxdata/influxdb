package tenant_test

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/kv"
	"github.com/influxdata/influxdb/tenant"
	influxdbtesting "github.com/influxdata/influxdb/testing"
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

	for _, m := range f.UserResourceMappings {
		if err := svc.CreateUserResourceMapping(context.Background(), m); err != nil {
			t.Fatalf("failed to populate mappings")
		}
	}

	return svc, func() {
		for _, m := range f.UserResourceMappings {
			if err := svc.DeleteUserResourceMapping(context.Background(), m.ResourceID, m.UserID); err != nil {
				t.Logf("failed to remove user resource mapping: %v", err)
			}
		}
	}
}
