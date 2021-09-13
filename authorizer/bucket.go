package authorizer

import (
	"context"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/tracing"
)

var _ influxdb.BucketService = (*BucketService)(nil)

// BucketService wraps a influxdb.BucketService and authorizes actions
// against it appropriately.
type BucketService struct {
	s influxdb.BucketService
}

// NewBucketService constructs an instance of an authorizing bucket service.
func NewBucketService(s influxdb.BucketService) *BucketService {
	return &BucketService{
		s: s,
	}
}

// FindBucketByID checks to see if the authorizer on context has read access to the id provided.
func (s *BucketService) FindBucketByID(ctx context.Context, id platform.ID) (*influxdb.Bucket, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	b, err := s.s.FindBucketByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if _, _, err := AuthorizeReadBucket(ctx, b.Type, b.ID, b.OrgID); err != nil {
		return nil, err
	}
	return b, nil
}

// FindBucketByName returns a bucket by name for a particular organization.
func (s *BucketService) FindBucketByName(ctx context.Context, orgID platform.ID, n string) (*influxdb.Bucket, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	b, err := s.s.FindBucketByName(ctx, orgID, n)
	if err != nil {
		return nil, err
	}
	if _, _, err := AuthorizeReadBucket(ctx, b.Type, b.ID, b.OrgID); err != nil {
		return nil, err
	}
	return b, nil
}

// FindBucket retrieves the bucket and checks to see if the authorizer on context has read access to the bucket.
func (s *BucketService) FindBucket(ctx context.Context, filter influxdb.BucketFilter) (*influxdb.Bucket, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	b, err := s.s.FindBucket(ctx, filter)
	if err != nil {
		return nil, err
	}
	if _, _, err := AuthorizeReadBucket(ctx, b.Type, b.ID, b.OrgID); err != nil {
		return nil, err
	}
	return b, nil
}

// FindBuckets retrieves all buckets that match the provided filter and then filters the list down to only the resources that are authorized.
func (s *BucketService) FindBuckets(ctx context.Context, filter influxdb.BucketFilter, opt ...influxdb.FindOptions) ([]*influxdb.Bucket, int, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	// TODO: we'll likely want to push this operation into the database eventually since fetching the whole list of data
	// will likely be expensive.
	bs, _, err := s.s.FindBuckets(ctx, filter, opt...)
	if err != nil {
		return nil, 0, err
	}
	return AuthorizeFindBuckets(ctx, bs)
}

// CreateBucket checks to see if the authorizer on context has write access to the global buckets resource.
func (s *BucketService) CreateBucket(ctx context.Context, b *influxdb.Bucket) error {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	if _, _, err := AuthorizeCreate(ctx, influxdb.BucketsResourceType, b.OrgID); err != nil {
		return err
	}
	return s.s.CreateBucket(ctx, b)
}

// UpdateBucket checks to see if the authorizer on context has write access to the bucket provided.
func (s *BucketService) UpdateBucket(ctx context.Context, id platform.ID, upd influxdb.BucketUpdate) (*influxdb.Bucket, error) {
	b, err := s.s.FindBucketByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if _, _, err := AuthorizeWrite(ctx, influxdb.BucketsResourceType, id, b.OrgID); err != nil {
		return nil, err
	}
	return s.s.UpdateBucket(ctx, id, upd)
}

// DeleteBucket checks to see if the authorizer on context has write access to the bucket provided.
func (s *BucketService) DeleteBucket(ctx context.Context, id platform.ID) error {
	b, err := s.s.FindBucketByID(ctx, id)
	if err != nil {
		return err
	}
	if _, _, err := AuthorizeWrite(ctx, influxdb.BucketsResourceType, id, b.OrgID); err != nil {
		return err
	}
	return s.s.DeleteBucket(ctx, id)
}
