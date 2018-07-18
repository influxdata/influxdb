package mock

import (
	"context"

	"github.com/influxdata/platform"
	"go.uber.org/zap"
)

// BucketService is a mock implementation of a retention.BucketService, which
// also makes it a suitable mock to use wherever an platform.BucketService is required.
type BucketService struct {
	// Methods for a retention.BucketService
	OpenFn       func() error
	CloseFn      func() error
	WithLoggerFn func(l *zap.Logger)

	// Methods for an platform.BucketService
	FindBucketByIDFn func(context.Context, platform.ID) (*platform.Bucket, error)
	FindBucketFn     func(context.Context, platform.BucketFilter) (*platform.Bucket, error)
	FindBucketsFn    func(context.Context, platform.BucketFilter, ...platform.FindOptions) ([]*platform.Bucket, int, error)
	CreateBucketFn   func(context.Context, *platform.Bucket) error
	UpdateBucketFn   func(context.Context, platform.ID, platform.BucketUpdate) (*platform.Bucket, error)
	DeleteBucketFn   func(context.Context, platform.ID) error
}

// NewBucketService returns a mock BucketService where its methods will return
// zero values.
func NewBucketService() *BucketService {
	return &BucketService{
		OpenFn:           func() error { return nil },
		CloseFn:          func() error { return nil },
		WithLoggerFn:     func(l *zap.Logger) {},
		FindBucketByIDFn: func(context.Context, platform.ID) (*platform.Bucket, error) { return nil, nil },
		FindBucketFn:     func(context.Context, platform.BucketFilter) (*platform.Bucket, error) { return nil, nil },
		FindBucketsFn: func(context.Context, platform.BucketFilter, ...platform.FindOptions) ([]*platform.Bucket, int, error) {
			return nil, 0, nil
		},
		CreateBucketFn: func(context.Context, *platform.Bucket) error { return nil },
		UpdateBucketFn: func(context.Context, platform.ID, platform.BucketUpdate) (*platform.Bucket, error) { return nil, nil },
		DeleteBucketFn: func(context.Context, platform.ID) error { return nil },
	}
}

// Open opens the BucketService.
func (s *BucketService) Open() error { return s.OpenFn() }

// Close closes the BucketService.
func (s *BucketService) Close() error { return s.CloseFn() }

// WithLogger sets the logger on the BucketService.
func (s *BucketService) WithLogger(l *zap.Logger) { s.WithLoggerFn(l) }

// FindBucketByID returns a single bucket by ID.
func (s *BucketService) FindBucketByID(ctx context.Context, id platform.ID) (*platform.Bucket, error) {
	return s.FindBucketByIDFn(ctx, id)
}

// FindBucket returns the first bucket that matches filter.
func (s *BucketService) FindBucket(ctx context.Context, filter platform.BucketFilter) (*platform.Bucket, error) {
	return s.FindBucketFn(ctx, filter)
}

// FindBuckets returns a list of buckets that match filter and the total count of matching buckets.
func (s *BucketService) FindBuckets(ctx context.Context, filter platform.BucketFilter, opts ...platform.FindOptions) ([]*platform.Bucket, int, error) {
	return s.FindBucketsFn(ctx, filter, opts...)
}

// CreateBucket creates a new bucket and sets b.ID with the new identifier.
func (s *BucketService) CreateBucket(ctx context.Context, bucket *platform.Bucket) error {
	return s.CreateBucketFn(ctx, bucket)
}

// UpdateBucket updates a single bucket with changeset.
func (s *BucketService) UpdateBucket(ctx context.Context, id platform.ID, upd platform.BucketUpdate) (*platform.Bucket, error) {
	return s.UpdateBucketFn(ctx, id, upd)
}

// DeleteBucket removes a bucket by ID.
func (s *BucketService) DeleteBucket(ctx context.Context, id platform.ID) error {
	return s.DeleteBucket(ctx, id)
}
