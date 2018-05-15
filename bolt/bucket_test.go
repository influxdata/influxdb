package bolt_test

import (
	"context"
	"testing"

	"github.com/influxdata/platform"
	platformtesting "github.com/influxdata/platform/testing"
)

func initBucketService(f platformtesting.BucketFields, t *testing.T) (platform.BucketService, func()) {
	c, closeFn, err := NewTestClient()
	if err != nil {
		t.Fatalf("failed to create new bolt client: %v", err)
	}
	c.IDGenerator = f.IDGenerator
	ctx := context.TODO()
	for _, u := range f.Buckets {
		if err := c.PutBucket(ctx, u); err != nil {
			t.Fatalf("failed to populate buckets")
		}
	}
	return c, func() {
		defer closeFn()
		for _, u := range f.Buckets {
			if err := c.DeleteBucket(ctx, u.ID); err != nil {
				t.Logf("failed to remove buckets: %v", err)
			}
		}
	}
}

func TestBucketService_CreateBucket(t *testing.T) {
	platformtesting.CreateBucket(initBucketService, t)
}

func TestBucketService_FindBucketByID(t *testing.T) {
	platformtesting.FindBucketByID(initBucketService, t)
}

func TestBucketService_FindBuckets(t *testing.T) {
	platformtesting.FindBuckets(initBucketService, t)
}

func TestBucketService_DeleteBucket(t *testing.T) {
	platformtesting.DeleteBucket(initBucketService, t)
}

func TestBucketService_FindBucket(t *testing.T) {
	platformtesting.FindBucket(initBucketService, t)
}

func TestBucketService_UpdateBucket(t *testing.T) {
	platformtesting.UpdateBucket(initBucketService, t)
}
