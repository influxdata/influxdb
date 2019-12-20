package inmem

import (
	"context"
	"testing"

	platform "github.com/influxdata/influxdb/v2"
	platformtesting "github.com/influxdata/influxdb/v2/testing"
)

func initBucketService(f platformtesting.BucketFields, t *testing.T) (platform.BucketService, string, func()) {
	s := NewService()
	s.IDGenerator = f.IDGenerator
	s.TimeGenerator = f.TimeGenerator
	if f.TimeGenerator == nil {
		s.TimeGenerator = platform.RealTimeGenerator{}
	}
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
	t.Skip("bucket service no longer used.  Remove all of this inmem stuff")
	platformtesting.BucketService(initBucketService, t)
}
