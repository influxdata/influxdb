package tenant_test

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/tenant"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
)

func TestInmemBucketService(t *testing.T) {
	influxdbtesting.BucketService(initInmemBucketService, t)
}

func initInmemBucketService(f influxdbtesting.BucketFields, t *testing.T) (influxdb.BucketService, string, func()) {
	s, closeBolt, err := influxdbtesting.NewTestInmemStore(t)
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
	if f.IDGenerator != nil {
		storage.IDGen = f.IDGenerator
	}

	if f.OrgIDs != nil {
		storage.OrgIDGen = f.OrgIDs
	}

	if f.BucketIDs != nil {
		storage.BucketIDGen = f.BucketIDs
	}

	// go direct to storage for test data
	if err := s.Update(context.Background(), func(tx kv.Tx) error {
		for _, o := range f.Organizations {
			if err := storage.CreateOrg(tx.Context(), tx, o); err != nil {
				return err
			}
		}

		for _, b := range f.Buckets {
			if err := storage.CreateBucket(tx.Context(), tx, b); err != nil {
				return err
			}
		}

		return nil
	}); err != nil {
		t.Fatalf("failed to populate organizations: %s", err)
	}

	return tenant.NewService(storage), "tenant/", func() {
		if err := s.Update(context.Background(), func(tx kv.Tx) error {
			for _, b := range f.Buckets {
				if err := storage.DeleteBucket(tx.Context(), tx, b.ID); err != nil {
					return err
				}
			}

			for _, o := range f.Organizations {
				if err := storage.DeleteOrg(tx.Context(), tx, o.ID); err != nil {
					return err
				}
			}

			return nil
		}); err != nil {
			t.Logf("failed to cleanup organizations: %s", err)
		}
	}
}

func TestBucketFind(t *testing.T) {
	s, close, err := influxdbtesting.NewTestInmemStore(t)
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
	s, close, err := influxdbtesting.NewTestInmemStore(t)
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
