package dbrp_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/dbrp"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/mock"
	itesting "github.com/influxdata/influxdb/v2/testing"
)

func initDBRPMappingService(f itesting.DBRPMappingFields, t *testing.T) (influxdb.DBRPMappingService, func()) {
	s, closeStore, err := itesting.NewTestBoltStore(t)
	if err != nil {
		t.Fatalf("failed to create new bolt kv store: %v", err)
	}

	if f.BucketSvc == nil {
		f.BucketSvc = &mock.BucketService{
			FindBucketByIDFn: func(ctx context.Context, id platform.ID) (*influxdb.Bucket, error) {
				// always find a bucket.
				return &influxdb.Bucket{
					ID:   id,
					Name: fmt.Sprintf("bucket-%v", id),
				}, nil
			},
		}
	}

	svc := dbrp.NewService(context.Background(), f.BucketSvc, s)

	if err := f.Populate(context.Background(), svc); err != nil {
		t.Fatal(err)
	}
	return svc, func() {
		if err := itesting.CleanupDBRPMappingsV2(context.Background(), svc); err != nil {
			t.Error(err)
		}
		closeStore()
	}
}

func TestBoltDBRPMappingService(t *testing.T) {
	t.Parallel()
	itesting.DBRPMappingService(initDBRPMappingService, t)
}
