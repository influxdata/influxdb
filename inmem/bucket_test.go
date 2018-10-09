package inmem

import (
	"context"
	"testing"

	"github.com/influxdata/platform"
	platformtesting "github.com/influxdata/platform/testing"
)

func initBucketService(f platformtesting.BucketFields, t *testing.T) (platform.BucketService, func()) {
	s := NewService()
	s.IDGenerator = f.IDGenerator
	ctx := context.Background()
	for _, o := range f.Organizations {
		if err := s.PutOrganization(ctx, o); err != nil {
			t.Fatalf("failed to populate organizations")
		}
	}
	for _, b := range f.Buckets {
		if err := s.PutBucket(ctx, b); err != nil {
			t.Fatalf("failed to populate buckets")
		}
	}
	return s, func() {}
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
