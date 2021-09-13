package tenant

import (
	"context"
	"fmt"
	"path"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/kit/tracing"
	"github.com/influxdata/influxdb/v2/pkg/httpc"
)

// BucketClientService connects to Influx via HTTP using tokens to manage buckets
type BucketClientService struct {
	Client *httpc.Client
	// OpPrefix is an additional property for error
	// find bucket service, when finds nothing.
	OpPrefix string
}

// FindBucketByName returns a single bucket by name
func (s *BucketClientService) FindBucketByName(ctx context.Context, orgID platform.ID, name string) (*influxdb.Bucket, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	if name == "" {
		return nil, &errors.Error{
			Code: errors.EUnprocessableEntity,
			Op:   s.OpPrefix + influxdb.OpFindBuckets,
			Msg:  "bucket name is required",
		}
	}

	bkts, n, err := s.FindBuckets(ctx, influxdb.BucketFilter{
		Name:           &name,
		OrganizationID: &orgID,
	})
	if err != nil {
		return nil, err
	}
	if n == 0 || len(bkts) == 0 {
		return nil, &errors.Error{
			Code: errors.ENotFound,
			Op:   s.OpPrefix + influxdb.OpFindBucket,
			Msg:  fmt.Sprintf("bucket %q not found", name),
		}
	}

	return bkts[0], nil
}

// FindBucketByID returns a single bucket by ID.
func (s *BucketClientService) FindBucketByID(ctx context.Context, id platform.ID) (*influxdb.Bucket, error) {
	// TODO(@jsteenb2): are tracing
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	var br bucketResponse
	err := s.Client.
		Get(path.Join(prefixBuckets, id.String())).
		DecodeJSON(&br).
		Do(ctx)
	if err != nil {
		return nil, err
	}
	return br.toInfluxDB(), nil
}

// FindBucket returns the first bucket that matches filter.
func (s *BucketClientService) FindBucket(ctx context.Context, filter influxdb.BucketFilter) (*influxdb.Bucket, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	bs, n, err := s.FindBuckets(ctx, filter)
	if err != nil {
		return nil, err
	}

	if n == 0 && filter.Name != nil {
		return nil, &errors.Error{
			Code: errors.ENotFound,
			Op:   s.OpPrefix + influxdb.OpFindBucket,
			Msg:  fmt.Sprintf("bucket %q not found", *filter.Name),
		}
	} else if n == 0 {
		return nil, &errors.Error{
			Code: errors.ENotFound,
			Op:   s.OpPrefix + influxdb.OpFindBucket,
			Msg:  "bucket not found",
		}
	}

	return bs[0], nil
}

// FindBuckets returns a list of buckets that match filter and the total count of matching buckets.
// Additional options provide pagination & sorting.
func (s *BucketClientService) FindBuckets(ctx context.Context, filter influxdb.BucketFilter, opt ...influxdb.FindOptions) ([]*influxdb.Bucket, int, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	params := influxdb.FindOptionParams(opt...)
	if filter.OrganizationID != nil {
		params = append(params, [2]string{"orgID", filter.OrganizationID.String()})
	}
	if filter.Org != nil {
		params = append(params, [2]string{"org", *filter.Org})
	}
	if filter.ID != nil {
		params = append(params, [2]string{"id", filter.ID.String()})
	}
	if filter.Name != nil {
		params = append(params, [2]string{"name", (*filter.Name)})
	}

	var bs bucketsResponse
	err := s.Client.
		Get(prefixBuckets).
		QueryParams(params...).
		DecodeJSON(&bs).
		Do(ctx)
	if err != nil {
		return nil, 0, err
	}
	buckets := make([]*influxdb.Bucket, 0, len(bs.Buckets))
	for _, b := range bs.Buckets {
		pb := b.bucket.toInfluxDB()
		buckets = append(buckets, pb)
	}

	return buckets, len(buckets), nil
}

// CreateBucket creates a new bucket and sets b.ID with the new identifier.
func (s *BucketClientService) CreateBucket(ctx context.Context, b *influxdb.Bucket) error {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	var br bucketResponse
	err := s.Client.
		PostJSON(newBucket(b), prefixBuckets).
		DecodeJSON(&br).
		Do(ctx)
	if err != nil {
		return err
	}

	pb := br.toInfluxDB()
	*b = *pb
	return nil
}

// UpdateBucket updates a single bucket with changeset.
// Returns the new bucket state after update.
func (s *BucketClientService) UpdateBucket(ctx context.Context, id platform.ID, upd influxdb.BucketUpdate) (*influxdb.Bucket, error) {
	var br bucketResponse
	err := s.Client.
		PatchJSON(newBucketUpdate(&upd), path.Join(prefixBuckets, id.String())).
		DecodeJSON(&br).
		Do(ctx)
	if err != nil {
		return nil, err
	}
	return br.toInfluxDB(), nil
}

// DeleteBucket removes a bucket by ID.
func (s *BucketClientService) DeleteBucket(ctx context.Context, id platform.ID) error {
	return s.Client.
		Delete(path.Join(prefixBuckets, id.String())).
		Do(ctx)
}
