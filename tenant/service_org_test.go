package tenant_test

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/tenant"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
)

func TestBoltOrganizationService(t *testing.T) {
	influxdbtesting.OrganizationService(initBoltOrganizationService, t)
}

func initBoltOrganizationService(f influxdbtesting.OrganizationFields, t *testing.T) (influxdb.OrganizationService, string, func()) {
	s, closeBolt, err := influxdbtesting.NewTestBoltStore(t)
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
	storage := tenant.NewStore(s)

	if f.OrgBucketIDs != nil {
		storage.OrgIDGen = f.OrgBucketIDs
		storage.BucketIDGen = f.OrgBucketIDs
	}

	// go direct to storage for test data
	if err := s.Update(context.Background(), func(tx kv.Tx) error {
		for _, o := range f.Organizations {
			if err := storage.CreateOrg(tx.Context(), tx, o); err != nil {
				return err
			}
		}

		return nil
	}); err != nil {
		t.Fatalf("failed to populate organizations: %s", err)
	}

	return tenant.NewService(storage), "tenant/", func() {
		// go direct to storage for test data
		if err := s.Update(context.Background(), func(tx kv.Tx) error {
			for _, o := range f.Organizations {
				if err := storage.DeleteOrg(tx.Context(), tx, o.ID); err != nil {
					return err
				}
			}

			return nil
		}); err != nil {
			t.Logf("failed to remove organizations: %v", err)
		}
	}
}
