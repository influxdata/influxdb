package mock

import (
	"context"
	"time"

	platform2 "github.com/influxdata/influxdb/v2/kit/platform"

	platform "github.com/influxdata/influxdb/v2"
)

// BucketService is a mock implementation of a retention.BucketService, which
// also makes it a suitable mock to use wherever an platform.BucketService is required.
type BucketService struct {
	// Methods for a retention.BucketService
	OpenFn  func() error
	CloseFn func() error

	// Methods for an platform.BucketService
	FindBucketByIDFn      func(context.Context, platform2.ID) (*platform.Bucket, error)
	FindBucketByIDCalls   SafeCount
	FindBucketByNameFn    func(context.Context, platform2.ID, string) (*platform.Bucket, error)
	FindBucketByNameCalls SafeCount
	FindBucketFn          func(context.Context, platform.BucketFilter) (*platform.Bucket, error)
	FindBucketCalls       SafeCount
	FindBucketsFn         func(context.Context, platform.BucketFilter, ...platform.FindOptions) ([]*platform.Bucket, int, error)
	FindBucketsCalls      SafeCount
	CreateBucketFn        func(context.Context, *platform.Bucket) error
	CreateBucketCalls     SafeCount
	UpdateBucketFn        func(context.Context, platform2.ID, platform.BucketUpdate) (*platform.Bucket, error)
	UpdateBucketCalls     SafeCount
	DeleteBucketFn        func(context.Context, platform2.ID) error
	DeleteBucketCalls     SafeCount
}

// NewBucketService returns a mock BucketService where its methods will return
// zero values.
func NewBucketService() *BucketService {
	return &BucketService{
		OpenFn:           func() error { return nil },
		CloseFn:          func() error { return nil },
		FindBucketByIDFn: func(context.Context, platform2.ID) (*platform.Bucket, error) { return nil, nil },
		FindBucketByNameFn: func(context.Context, platform2.ID, string) (*platform.Bucket, error) {
			return &platform.Bucket{
				ID:              platform2.ID(10),
				Type:            platform.BucketTypeSystem,
				Name:            "_tasks",
				RetentionPeriod: time.Hour * 24 * 3,
				Description:     "System bucket for task logs",
			}, nil
		},
		FindBucketFn: func(context.Context, platform.BucketFilter) (*platform.Bucket, error) { return nil, nil },
		FindBucketsFn: func(context.Context, platform.BucketFilter, ...platform.FindOptions) ([]*platform.Bucket, int, error) {
			return nil, 0, nil
		},
		CreateBucketFn: func(context.Context, *platform.Bucket) error { return nil },
		UpdateBucketFn: func(context.Context, platform2.ID, platform.BucketUpdate) (*platform.Bucket, error) { return nil, nil },
		DeleteBucketFn: func(context.Context, platform2.ID) error { return nil },
	}
}

// Open opens the BucketService.
func (s *BucketService) Open() error { return s.OpenFn() }

// Close closes the BucketService.
func (s *BucketService) Close() error { return s.CloseFn() }

// FindBucketByID returns a single bucket by ID.
func (s *BucketService) FindBucketByID(ctx context.Context, id platform2.ID) (*platform.Bucket, error) {
	defer s.FindBucketByIDCalls.IncrFn()()
	return s.FindBucketByIDFn(ctx, id)
}

// FindBucketByName returns a single bucket by name.
func (s *BucketService) FindBucketByName(ctx context.Context, orgID platform2.ID, name string) (*platform.Bucket, error) {
	defer s.FindBucketByNameCalls.IncrFn()()
	return s.FindBucketByNameFn(ctx, orgID, name)
}

// FindBucket returns the first bucket that matches filter.
func (s *BucketService) FindBucket(ctx context.Context, filter platform.BucketFilter) (*platform.Bucket, error) {
	defer s.FindBucketCalls.IncrFn()()
	return s.FindBucketFn(ctx, filter)
}

// FindBuckets returns a list of buckets that match filter and the total count of matching buckets.
func (s *BucketService) FindBuckets(ctx context.Context, filter platform.BucketFilter, opts ...platform.FindOptions) ([]*platform.Bucket, int, error) {
	defer s.FindBucketsCalls.IncrFn()()
	return s.FindBucketsFn(ctx, filter, opts...)
}

// CreateBucket creates a new bucket and sets b.ID with the new identifier.
func (s *BucketService) CreateBucket(ctx context.Context, bucket *platform.Bucket) error {
	defer s.CreateBucketCalls.IncrFn()()
	return s.CreateBucketFn(ctx, bucket)
}

// UpdateBucket updates a single bucket with changeset.
func (s *BucketService) UpdateBucket(ctx context.Context, id platform2.ID, upd platform.BucketUpdate) (*platform.Bucket, error) {
	defer s.UpdateBucketCalls.IncrFn()()
	return s.UpdateBucketFn(ctx, id, upd)
}

// DeleteBucket removes a bucket by ID.
func (s *BucketService) DeleteBucket(ctx context.Context, id platform2.ID) error {
	defer s.DeleteBucketCalls.IncrFn()()
	return s.DeleteBucketFn(ctx, id)
}
