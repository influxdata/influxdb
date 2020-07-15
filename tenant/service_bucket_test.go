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

func initBucketService(s kv.SchemaStore, f influxdbtesting.BucketFields, t *testing.T) (influxdb.BucketService, string, func()) {
	storage := tenant.NewStore(s)
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

func TestBucketFind(t *testing.T) {
	s, close, err := NewTestInmemStore(t)
	if err != nil {
		t.Fatal(err)
	}
	defer close()

	storage := tenant.NewStore(s)
	svc := tenant.NewService(storage)
	o := &influxdb.Organization{
		Name: "theorg",
	}

	if err := svc.CreateOrganization(context.Background(), o); err != nil {
		t.Fatal(err)
	}
	name := "thebucket"
	_, _, err = svc.FindBuckets(context.Background(), influxdb.BucketFilter{
		Name: &name,
		Org:  &o.Name,
	})
	if err.Error() != `bucket "thebucket" not found` {
		t.Fatal(err)
	}
}

func TestSystemBucketsInNameFind(t *testing.T) {
	s, close, err := NewTestInmemStore(t)
	if err != nil {
		t.Fatal(err)
	}
	defer close()

	storage := tenant.NewStore(s)
	svc := tenant.NewService(storage)
	o := &influxdb.Organization{
		Name: "theorg",
	}

	if err := svc.CreateOrganization(context.Background(), o); err != nil {
		t.Fatal(err)
	}
	b := &influxdb.Bucket{
		OrgID: o.ID,
		Name:  "thebucket",
	}
	if err := svc.CreateBucket(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	name := "thebucket"
	buckets, _, _ := svc.FindBuckets(context.Background(), influxdb.BucketFilter{
		Name: &name,
		Org:  &o.Name,
	})
	if len(buckets) != 1 {
		t.Fatal("failed to return a single bucket when doing a bucket lookup by name")
	}
}
