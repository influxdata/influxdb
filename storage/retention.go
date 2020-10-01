package storage

import (
	"context"

	"github.com/influxdata/influxdb/v2"
)

// A BucketFinder is responsible for providing access to buckets via a filter.
type BucketFinder interface {
	FindBuckets(context.Context, influxdb.BucketFilter, ...influxdb.FindOptions) ([]*influxdb.Bucket, int, error)
}
