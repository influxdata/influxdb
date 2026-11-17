package dbrp_test

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/dbrp"
	"github.com/influxdata/influxdb/v2/mock"
	itesting "github.com/influxdata/influxdb/v2/testing"
)

func initDBRPMappingService(f itesting.DBRPMappingFields, t *testing.T) (influxdb.DBRPMappingService, func()) {
	s, closeStore := itesting.NewTestBoltStore(t)
	if f.BucketSvc == nil {
		f.BucketSvc = &mock.BucketService{
			FindBucketByIDFn: itesting.FindDBRPMappingTestBucketByID,
			FindBucketsFn: func(ctx context.Context, bf influxdb.BucketFilter, fo ...influxdb.FindOptions) ([]*influxdb.Bucket, int, error) {
				return []*influxdb.Bucket{}, 0, nil
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
