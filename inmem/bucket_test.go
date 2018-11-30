package inmem

import (
	"context"
	"testing"

	"github.com/influxdata/platform"
	platformtesting "github.com/influxdata/platform/testing"
)

func initBucketService(f platformtesting.BucketFields, t *testing.T) (platform.BucketService, string, func()) {
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
	return s, OpPrefix, func() {}
}

func TestBucketService(t *testing.T) {
	platformtesting.BucketService(initBucketService, t)
}
