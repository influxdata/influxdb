package authorizer

import (
	"context"
	"fmt"

	icontext "github.com/influxdata/influxdb/context"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/kit/tracing"
)

var _ influxdb.BucketService = (*BucketService)(nil)

// BucketService wraps a influxdb.BucketService and authorizes actions
// against it appropriately.
type BucketService struct {
	s influxdb.BucketService
	u influxdb.UserResourceMappingService
}

// NewBucketService constructs an instance of an authorizing bucket serivce.
func NewBucketService(s influxdb.BucketService, u influxdb.UserResourceMappingService) *BucketService {
	return &BucketService{
		s: s,
		u: u,
	}
}

func newBucketPermission(a influxdb.Action, orgID, id influxdb.ID) (*influxdb.Permission, error) {
	return influxdb.NewPermissionAtID(id, a, influxdb.BucketsResourceType, orgID)
}

func authorizeWriteBucket(ctx context.Context, orgID, id influxdb.ID) error {
	p, err := newBucketPermission(influxdb.WriteAction, orgID, id)
	if err != nil {
		return err
	}

	if err := IsAllowed(ctx, *p); err != nil {
		return err
	}

	return nil
}

func authorizeReadBucket(ctx context.Context, b *influxdb.Bucket, u influxdb.UserResourceMappingService) error {
	switch b.Type {
	case influxdb.BucketTypeSystem:
		return authorizeReadSystemBucket(ctx, b, u)
	default:
		return authorizeReadUserBucket(ctx, b)
	}
}

func authorizeReadUserBucket(ctx context.Context, b *influxdb.Bucket) error {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	p, err := newBucketPermission(influxdb.ReadAction, b.OrgID, b.ID)
	if err != nil {
		return err
	}

	if err := IsAllowed(ctx, *p); err != nil {
		return err
	}

	return nil
}

func authorizeReadSystemBucket(ctx context.Context, b *influxdb.Bucket, u influxdb.UserResourceMappingService) error {
	// HACK: remove once system buckets are migrated away from hard coded values
	if !b.OrgID.Valid() && (b.ID == influxdb.TasksSystemBucketID || b.ID == influxdb.MonitoringSystemBucketID) {
		return nil
	}

	userID, err := icontext.GetUserID(ctx)
	if err != nil {
		return &influxdb.Error{
			Code: influxdb.EUnauthorized,
			Msg:  fmt.Sprintf("unauthorized"),
			Err:  err,
		}
	}

	ms, _, err := u.FindUserResourceMappings(ctx, influxdb.UserResourceMappingFilter{
		UserID:       userID,
		ResourceType: influxdb.OrgsResourceType,
	})
	if err != nil {
		return fmt.Errorf("finding organization mapping for user %s: %v", b.ID, err)
	}

	for _, m := range ms {
		if m.ResourceID == b.OrgID {
			return nil
		}
	}

	return &influxdb.Error{
		Code: influxdb.EUnauthorized,
		Msg:  fmt.Sprintf("unauthorized"),
	}
}

// FindBucketByID checks to see if the authorizer on context has read access to the id provided.
func (s *BucketService) FindBucketByID(ctx context.Context, id influxdb.ID) (*influxdb.Bucket, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	b, err := s.s.FindBucketByID(ctx, id)
	if err != nil {
		return nil, err
	}

	if err := authorizeReadBucket(ctx, b, s.u); err != nil {
		return nil, err
	}

	return b, nil
}

// FindBucketByName returns a bucket by name for a particular organization.
func (s *BucketService) FindBucketByName(ctx context.Context, orgID influxdb.ID, n string) (*influxdb.Bucket, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	b, err := s.s.FindBucketByName(ctx, orgID, n)
	if err != nil {
		return nil, err
	}

	if err := authorizeReadBucket(ctx, b, s.u); err != nil {
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

	if err := authorizeReadBucket(ctx, b, s.u); err != nil {
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

	// This filters without allocating
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	buckets := bs[:0]
	for _, b := range bs {
		err := authorizeReadBucket(ctx, b, s.u)
		if err != nil && influxdb.ErrorCode(err) != influxdb.EUnauthorized {
			return nil, 0, err
		}

		if influxdb.ErrorCode(err) == influxdb.EUnauthorized {
			continue
		}

		buckets = append(buckets, b)
	}

	return buckets, len(buckets), nil
}

// CreateBucket checks to see if the authorizer on context has write access to the global buckets resource.
func (s *BucketService) CreateBucket(ctx context.Context, b *influxdb.Bucket) error {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	p, err := influxdb.NewPermission(influxdb.WriteAction, influxdb.BucketsResourceType, b.OrgID)
	if err != nil {
		return err
	}

	if err := IsAllowed(ctx, *p); err != nil {
		return err
	}

	return s.s.CreateBucket(ctx, b)
}

// UpdateBucket checks to see if the authorizer on context has write access to the bucket provided.
func (s *BucketService) UpdateBucket(ctx context.Context, id influxdb.ID, upd influxdb.BucketUpdate) (*influxdb.Bucket, error) {
	b, err := s.s.FindBucketByID(ctx, id)
	if err != nil {
		return nil, err
	}

	if err := authorizeWriteBucket(ctx, b.OrgID, id); err != nil {
		return nil, err
	}

	return s.s.UpdateBucket(ctx, id, upd)
}

// DeleteBucket checks to see if the authorizer on context has write access to the bucket provided.
func (s *BucketService) DeleteBucket(ctx context.Context, id influxdb.ID) error {
	b, err := s.s.FindBucketByID(ctx, id)
	if err != nil {
		return err
	}

	if err := authorizeWriteBucket(ctx, b.OrgID, id); err != nil {
		return err
	}

	return s.s.DeleteBucket(ctx, id)
}
