package influxdb

import (
	"context"

	"github.com/influxdata/platform"
)

// BucketService connects to Influx via HTTP using tokens to manage buckets
type BucketService struct {
	Addr               string
	Username           string
	Password           string
	InsecureSkipVerify bool
}

func (s *BucketService) FindBucketByID(ctx context.Context, id platform.ID) (*platform.Bucket, error) {
	panic("not implemented")
}

func (s *BucketService) FindBucket(ctx context.Context, filter platform.BucketFilter) (*platform.Bucket, error) {
	panic("not implemented")
}

func (s *BucketService) FindBuckets(ctx context.Context, filter platform.BucketFilter, opt ...platform.FindOptions) ([]*platform.Bucket, int, error) {
	panic("not implemented")
}

func (s *BucketService) CreateBucket(ctx context.Context, b *platform.Bucket) error {
	panic("not implemented")
}

func (s *BucketService) UpdateBucket(ctx context.Context, id platform.ID, upd platform.BucketUpdate) (*platform.Bucket, error) {
	panic("not implemented")
}

func (s *BucketService) DeleteBucket(ctx context.Context, id platform.ID) error {
	panic("not implemented")
}
