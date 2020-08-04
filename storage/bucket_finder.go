package storage

import (
	"context"

	"github.com/influxdata/influxdb/v2"
)

var _ BucketFinder = (*AllBucketsFinder)(nil)

// AllBucketsFinder is responsible for iterating over the paginated reponses from FindBuckets to return every bucket
type AllBucketsFinder struct {
	bucketSvc influxdb.BucketService
}

func NewAllBucketsFinder(bucketSvc influxdb.BucketService) *AllBucketsFinder {
	return &AllBucketsFinder{
		bucketSvc: bucketSvc,
	}
}

func (p *AllBucketsFinder) FindBuckets(ctx context.Context, filter influxdb.BucketFilter) ([]*influxdb.Bucket, int, error) {
	buckets := []*influxdb.Bucket{}
	o := influxdb.FindOptions{Limit: influxdb.MaxPageSize}

	res, n, err := p.bucketSvc.FindBuckets(ctx, filter, o)
	if err != nil {
		return buckets, 0, err
	}
	buckets = append(buckets, res...)

	// used to filter out duplicate system buckets
	foundTask := false
	foundMonitoring := false

	if n == o.Limit {
		for {
			o.Offset = o.Offset + o.Limit
			res, n, err := p.bucketSvc.FindBuckets(ctx, filter, o)
			if err != nil {
				return []*influxdb.Bucket{}, 0, err
			}
			for i := 0; i < len(res); i++ {
				if res[i].Name == "_tasks" && !foundTask {
					buckets = append(buckets, res[i])
					foundTask = true
				} else if res[i].Name == "_monitoring" && !foundMonitoring {
					buckets = append(buckets, res[i])
					foundMonitoring = true
				} else {
					buckets = append(buckets, res[i])
				}
			}

			if n < o.Limit {
				break
			}
		}
	}

	return buckets, len(buckets), nil
}
