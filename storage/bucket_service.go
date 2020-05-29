package storage

import (
	"context"
	"errors"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/tracing"
)

// BucketDeleter defines the behaviour of deleting a bucket.
type BucketDeleter interface {
	DeleteBucket(context.Context, influxdb.ID, influxdb.ID) error
}

// BucketService wraps an existing influxdb.BucketService implementation.
//
// BucketService ensures that when a bucket is deleted, all stored data
// associated with the bucket is either removed, or marked to be removed via a
// future compaction.
type BucketService struct {
	inner  influxdb.BucketService
	engine BucketDeleter
}

// NewBucketService returns a new BucketService for the provided BucketDeleter,
// which typically will be an Engine.
func NewBucketService(s influxdb.BucketService, engine BucketDeleter) *BucketService {
	return &BucketService{
		inner:  s,
		engine: engine,
	}
}

// FindBucketByID returns a single bucket by ID.
func (s *BucketService) FindBucketByID(ctx context.Context, id influxdb.ID) (*influxdb.Bucket, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	if s.inner == nil || s.engine == nil {
		return nil, errors.New("nil inner BucketService or Engine")
	}
	return s.inner.FindBucketByID(ctx, id)
}

// FindBucketByName returns a single bucket by name.
func (s *BucketService) FindBucketByName(ctx context.Context, orgID influxdb.ID, name string) (*influxdb.Bucket, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	if s.inner == nil || s.engine == nil {
		return nil, errors.New("nil inner BucketService or Engine")
	}
	return s.inner.FindBucketByName(ctx, orgID, name)
}

// FindBucket returns the first bucket that matches filter.
func (s *BucketService) FindBucket(ctx context.Context, filter influxdb.BucketFilter) (*influxdb.Bucket, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	if s.inner == nil || s.engine == nil {
		return nil, errors.New("nil inner BucketService or Engine")
	}
	return s.inner.FindBucket(ctx, filter)
}

// FindBuckets returns a list of buckets that match filter and the total count of matching buckets.
// Additional options provide pagination & sorting.
func (s *BucketService) FindBuckets(ctx context.Context, filter influxdb.BucketFilter, opt ...influxdb.FindOptions) ([]*influxdb.Bucket, int, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	if s.inner == nil || s.engine == nil {
		return nil, 0, errors.New("nil inner BucketService or Engine")
	}
	return s.inner.FindBuckets(ctx, filter, opt...)
}

// CreateBucket creates a new bucket and sets b.ID with the new identifier.
func (s *BucketService) CreateBucket(ctx context.Context, b *influxdb.Bucket) error {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	if s.inner == nil || s.engine == nil {
		return errors.New("nil inner BucketService or Engine")
	}
	return s.inner.CreateBucket(ctx, b)
}

// UpdateBucket updates a single bucket with changeset.
// Returns the new bucket state after update.
func (s *BucketService) UpdateBucket(ctx context.Context, id influxdb.ID, upd influxdb.BucketUpdate) (*influxdb.Bucket, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	if s.inner == nil || s.engine == nil {
		return nil, errors.New("nil inner BucketService or Engine")
	}
	return s.inner.UpdateBucket(ctx, id, upd)
}

// DeleteBucket removes a bucket by ID.
func (s *BucketService) DeleteBucket(ctx context.Context, bucketID influxdb.ID) error {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	bucket, err := s.FindBucketByID(ctx, bucketID)
	if err != nil {
		return err
	}

	// The data is dropped first from the storage engine. If this fails for any
	// reason, then the bucket will still be available in the future to retrieve
	// the orgID, which is needed for the engine.
	if err := s.engine.DeleteBucket(ctx, bucket.OrgID, bucketID); err != nil {
		return err
	}
	return s.inner.DeleteBucket(ctx, bucketID)
}
