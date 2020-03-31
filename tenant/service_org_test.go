package tenant_test

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/kv"
	"github.com/influxdata/influxdb/tenant"
	influxdbtesting "github.com/influxdata/influxdb/testing"
)

func TestBoltOrganizationService(t *testing.T) {
	influxdbtesting.OrganizationService(initBoltOrganizationService, t)
}

func initBoltOrganizationService(f influxdbtesting.OrganizationFields, t *testing.T) (influxdb.OrganizationService, string, func()) {
	s, closeBolt, err := NewTestBoltStore(t)
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
	storage, err := tenant.NewStore(s)
	if err != nil {
		t.Fatal(err)
	}
	svc := tenant.NewService(storage)

	for _, o := range f.Organizations {
		if err := svc.CreateOrganization(context.Background(), o); err != nil {
			t.Fatalf("failed to populate organizations")
		}
	}

	return svc, "tenant/", func() {
		for _, o := range f.Organizations {
			if err := svc.DeleteOrganization(context.Background(), o.ID); err != nil {
				t.Logf("failed to remove organizations: %v", err)
			}
		}
	}
}
