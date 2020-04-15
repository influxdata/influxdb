package tenant_test

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/tenant"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
)

func TestBoltBucketService(t *testing.T) {
	influxdbtesting.BucketService(initBoltBucketService, t, influxdbtesting.WithoutHooks())
}

func initBoltBucketService(f influxdbtesting.BucketFields, t *testing.T) (influxdb.BucketService, string, func()) {
	s, closeBolt, err := NewTestInmemStore(t)
	if err != nil {
		t.Fatalf("failed to create new kv store: %v", err)
	}

	svc, op, closeSvc := initBucketService(s, f, t)
	return svc, op, func() {
		closeSvc()
		closeBolt()
	}
}

func initBucketService(s kv.Store, f influxdbtesting.BucketFields, t *testing.T) (influxdb.BucketService, string, func()) {
	storage, err := tenant.NewStore(s)
	if err != nil {
		t.Fatal(err)
	}

	svc := tenant.NewService(storage)

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

	return svc, "tenant/", func() {
		for _, o := range f.Organizations {
			if err := svc.DeleteOrganization(context.Background(), o.ID); err != nil {
				t.Logf("failed to remove organization: %v", err)
			}
		}
	}
}
