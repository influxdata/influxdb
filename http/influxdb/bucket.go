package influxdb

import (
	"context"
	"fmt"
	"time"

	platform2 "github.com/influxdata/influxdb/v2/kit/platform"

	platform "github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/tracing"
)

// BucketService connects to Influx via HTTP using tokens to manage buckets
type BucketService struct {
	Source *platform.Source
}

func (s *BucketService) FindBucketByName(ctx context.Context, orgID platform2.ID, n string) (*platform.Bucket, error) {
	return nil, fmt.Errorf("not supported")
}

func (s *BucketService) FindBucketByID(ctx context.Context, id platform2.ID) (*platform.Bucket, error) {
	return nil, fmt.Errorf("not supported")
}

func (s *BucketService) FindBucket(ctx context.Context, filter platform.BucketFilter) (*platform.Bucket, error) {
	return nil, fmt.Errorf("not supported")
}

func (s *BucketService) FindBuckets(ctx context.Context, filter platform.BucketFilter, opt ...platform.FindOptions) ([]*platform.Bucket, int, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	c, err := newClient(s.Source)
	if err != nil {
		return nil, 0, err
	}

	dbs, err := c.AllDB(ctx)
	if err != nil {
		return nil, 0, err
	}

	bs := []*platform.Bucket{}
	for _, db := range dbs {
		rps, err := c.AllRP(ctx, db.Name)
		if err != nil {
			return nil, 0, err
		}
		for _, rp := range rps {
			d, err := time.ParseDuration(rp.Duration)
			if err != nil {
				return nil, 0, err
			}
			b := &platform.Bucket{
				// TODO(desa): what to do about IDs?
				RetentionPeriod:     d,
				Name:                db.Name,
				RetentionPolicyName: rp.Name,
			}

			bs = append(bs, b)
		}
	}

	return bs, len(bs), nil
}

func (s *BucketService) CreateBucket(ctx context.Context, b *platform.Bucket) error {
	return fmt.Errorf("not supported")
}

func (s *BucketService) UpdateBucket(ctx context.Context, id platform2.ID, upd platform.BucketUpdate) (*platform.Bucket, error) {
	return nil, fmt.Errorf("not supported")
}

func (s *BucketService) DeleteBucket(ctx context.Context, id platform2.ID) error {
	return fmt.Errorf("not supported")
}
