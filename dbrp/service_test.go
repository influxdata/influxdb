package dbrp_test

import (
	"context"
	"errors"
	"fmt"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"io/ioutil"
	"os"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/bolt"
	"github.com/influxdata/influxdb/v2/dbrp"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/kv/migration/all"
	"github.com/influxdata/influxdb/v2/mock"
	itesting "github.com/influxdata/influxdb/v2/testing"
	"go.uber.org/zap/zaptest"
)

func NewTestBoltStore(t *testing.T) (kv.Store, func(), error) {
	t.Helper()

	f, err := ioutil.TempFile("", "influxdata-bolt-")
	if err != nil {
		return nil, nil, errors.New("unable to open temporary boltdb file")
	}
	f.Close()

	ctx := context.Background()
	logger := zaptest.NewLogger(t)
	path := f.Name()
	s := bolt.NewKVStore(logger, path, bolt.WithNoSync)
	if err := s.Open(context.Background()); err != nil {
		return nil, nil, err
	}

	if err := all.Up(ctx, logger, s); err != nil {
		return nil, nil, err
	}

	close := func() {
		s.Close()
		os.Remove(path)
	}

	return s, close, nil
}

func initDBRPMappingService(f itesting.DBRPMappingFieldsV2, t *testing.T) (influxdb.DBRPMappingServiceV2, func()) {
	s, closeStore, err := NewTestBoltStore(t)
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

func TestBoltDBRPMappingServiceV2(t *testing.T) {
	t.Parallel()
	itesting.DBRPMappingServiceV2(initDBRPMappingService, t)
}
