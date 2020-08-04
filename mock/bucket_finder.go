package mock

import (
	"context"

	"github.com/influxdata/influxdb/v2"
)

// BucketFinder is a mock implementation of storage.BucketFinder
type BucketFinder struct {
	// Methods for a retention.BucketFinder
	OpenFn           func() error
	CloseFn          func() error
	FindBucketsFn    func(context.Context, influxdb.BucketFilter) ([]*influxdb.Bucket, int, error)
	FindBucketsCalls SafeCount
}

// NewBucketFinder returns a mock BucketFinder where its methods will return
// zero values.
func NewBucketFinder() *BucketFinder {
	return &BucketFinder{
		OpenFn:  func() error { return nil },
		CloseFn: func() error { return nil },
		FindBucketsFn: func(context.Context, influxdb.BucketFilter) ([]*influxdb.Bucket, int, error) {
			return nil, 0, nil
		},
	}
}

// Open opens the BucketFinder.
func (s *BucketFinder) Open() error { return s.OpenFn() }

// Close closes the BucketFinder.
func (s *BucketFinder) Close() error { return s.CloseFn() }

// FindBuckets returns a list of buckets that match filter and the total count of matching buckets.
func (s *BucketFinder) FindBuckets(ctx context.Context, filter influxdb.BucketFilter) ([]*influxdb.Bucket, int, error) {
	defer s.FindBucketsCalls.IncrFn()()
	return s.FindBucketsFn(ctx, filter)
}
