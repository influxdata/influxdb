package storage_test

import (
	"context"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"testing"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/golang/mock/gomock"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/inmem"
	"github.com/influxdata/influxdb/v2/kv/migration/all"
	"github.com/influxdata/influxdb/v2/storage"
	"github.com/influxdata/influxdb/v2/storage/mocks"
	"github.com/influxdata/influxdb/v2/tenant"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

func TestBucketService_CreateBucket(t *testing.T) {
	testCases := []struct {
		name  string
		rp    time.Duration
		inSg  time.Duration
		outSg time.Duration
	}{
		{
			name:  "infinite RP, derived SGD",
			rp:    influxdb.InfiniteRetention,
			inSg:  0,
			outSg: humanize.Week,
		},
		{
			name:  "infinite RP, pinned SGD",
			rp:    influxdb.InfiniteRetention,
			inSg:  time.Hour,
			outSg: time.Hour,
		},
		{
			name:  "large RP, derived SGD",
			rp:    humanize.Week,
			inSg:  0,
			outSg: humanize.Day,
		},
		{
			name:  "large RP, pinned SGD",
			rp:    humanize.Week,
			inSg:  5 * time.Hour,
			outSg: 5 * time.Hour,
		},
		{
			name:  "small RP, derived SGD",
			rp:    humanize.Day,
			inSg:  0,
			outSg: time.Hour,
		},
		{
			name:  "small RP, pinned SGD",
			rp:    humanize.Day,
			inSg:  5 * time.Hour,
			outSg: 5 * time.Hour,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			engine := mocks.NewMockEngineSchema(ctrl)
			logger := zaptest.NewLogger(t)
			inmemService := newTenantService(t, logger)
			service := storage.NewBucketService(logger, inmemService, engine)
			ctx := context.Background()

			org := &influxdb.Organization{Name: "org1"}
			require.NoError(t, inmemService.CreateOrganization(ctx, org))

			bucket := &influxdb.Bucket{OrgID: org.ID, RetentionPeriod: tc.rp, ShardGroupDuration: tc.inSg}

			// Test creating a bucket calls into the creator.
			engine.EXPECT().CreateBucket(gomock.Any(), bucket)
			require.NoError(t, service.CreateBucket(ctx, bucket))

			// Test that a shard-group duration was created for the bucket
			require.Equal(t, tc.outSg, bucket.ShardGroupDuration)

			// Test that the shard-group duration was recorded in the KV store.
			kvBucket, err := inmemService.FindBucketByID(ctx, bucket.ID)
			require.NoError(t, err)
			require.Equal(t, tc.outSg, kvBucket.ShardGroupDuration)
		})
	}
}

func TestBucketService_DeleteBucket(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	engine := mocks.NewMockEngineSchema(ctrl)
	logger := zaptest.NewLogger(t)
	inmemService := newTenantService(t, logger)
	service := storage.NewBucketService(logger, inmemService, engine)
	ctx := context.Background()

	org := &influxdb.Organization{Name: "org1"}
	require.NoError(t, inmemService.CreateOrganization(ctx, org))

	bucket := &influxdb.Bucket{OrgID: org.ID}
	require.NoError(t, inmemService.CreateBucket(ctx, bucket))

	// Test deleting a bucket calls into the deleter.
	engine.EXPECT().DeleteBucket(gomock.Any(), org.ID, bucket.ID)
	require.NoError(t, service.DeleteBucket(ctx, bucket.ID))
}

func TestBucketService_DeleteNonexistentBucket(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	i, err := platform.IDFromString("2222222222222222")
	require.NoError(t, err)

	engine := mocks.NewMockEngineSchema(ctrl)
	logger := zaptest.NewLogger(t)
	inmemService := newTenantService(t, logger)
	service := storage.NewBucketService(logger, inmemService, engine)
	ctx := context.Background()

	require.Error(t, service.DeleteBucket(ctx, *i))
}

func newTenantService(t *testing.T, logger *zap.Logger) *tenant.Service {
	t.Helper()

	store := inmem.NewKVStore()
	if err := all.Up(context.Background(), logger, store); err != nil {
		t.Fatal(err)
	}

	return tenant.NewService(tenant.NewStore(store))
}
