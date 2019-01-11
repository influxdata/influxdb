package influxdb

import (
	"context"
	"fmt"
	"time"

	platform "github.com/influxdata/influxdb"
)

// BucketService connects to Influx via HTTP using tokens to manage buckets
type BucketService struct {
	Source *platform.Source
}

func (s *BucketService) FindBucketByID(ctx context.Context, id platform.ID) (*platform.Bucket, error) {
	return nil, fmt.Errorf("not supported")
}

func (s *BucketService) FindBucket(ctx context.Context, filter platform.BucketFilter) (*platform.Bucket, error) {
	return nil, fmt.Errorf("not supported")
}

func (s *BucketService) FindBuckets(ctx context.Context, filter platform.BucketFilter, opt ...platform.FindOptions) ([]*platform.Bucket, int, error) {
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

func (s *BucketService) UpdateBucket(ctx context.Context, id platform.ID, upd platform.BucketUpdate) (*platform.Bucket, error) {
	return nil, fmt.Errorf("not supported")
}

func (s *BucketService) DeleteBucket(ctx context.Context, id platform.ID) error {
	return fmt.Errorf("not supported")
}
