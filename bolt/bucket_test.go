package bolt_test

import (
	"context"
	"testing"

	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/bolt"
	platformtesting "github.com/influxdata/influxdb/testing"
)

func initBucketService(f platformtesting.BucketFields, t *testing.T) (platform.BucketService, string, func()) {
	c, closeFn, err := NewTestClient()
	if err != nil {
		t.Fatalf("failed to create new bolt client: %v", err)
	}
	c.IDGenerator = f.IDGenerator
	ctx := context.TODO()
	for _, o := range f.Organizations {
		if err := c.PutOrganization(ctx, o); err != nil {
			t.Fatalf("failed to populate organizations")
		}
	}
	for _, b := range f.Buckets {
		if err := c.PutBucket(ctx, b); err != nil {
			t.Fatalf("failed to populate buckets")
		}
	}
	return c, bolt.OpPrefix, func() {
		defer closeFn()
		for _, o := range f.Organizations {
			if err := c.DeleteOrganization(ctx, o.ID); err != nil {
				t.Logf("failed to remove organization: %v", err)
			}
		}
		for _, b := range f.Buckets {
			if err := c.DeleteBucket(ctx, b.ID); err != nil {
				t.Logf("failed to remove bucket: %v", err)
			}
		}
	}
}

func TestBucketService(t *testing.T) {
	platformtesting.BucketService(initBucketService, t)
}
