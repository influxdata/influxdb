package storage_test

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/inmem"
	"github.com/influxdata/influxdb/v2/kv/migration/all"
	"github.com/influxdata/influxdb/v2/storage"
	"github.com/influxdata/influxdb/v2/storage/mocks"
	"github.com/influxdata/influxdb/v2/tenant"
	"go.uber.org/zap/zaptest"
)

func TestBucketService(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	i, err := influxdb.IDFromString("2222222222222222")
	if err != nil {
		panic(err)
	}

	engine := mocks.NewMockEngineSchema(ctrl)

	logger := zaptest.NewLogger(t)
	inmemService := newTenantService(t)
	service := storage.NewBucketService(logger, inmemService, engine)

	if err := service.DeleteBucket(context.TODO(), *i); err == nil {
		t.Fatal("expected error, got nil")
	}

	org := &influxdb.Organization{Name: "org1"}
	if err := inmemService.CreateOrganization(context.TODO(), org); err != nil {
		panic(err)
	}

	bucket := &influxdb.Bucket{OrgID: org.ID}
	if err := inmemService.CreateBucket(context.TODO(), bucket); err != nil {
		panic(err)
	}

	engine.EXPECT().DeleteBucket(gomock.Any(), org.ID, bucket.ID)

	// Test deleting a bucket calls into the deleter.
	service = storage.NewBucketService(logger, inmemService, engine)

	if err := service.DeleteBucket(context.TODO(), bucket.ID); err != nil {
		t.Fatal(err)
	}
}

func newTenantService(t *testing.T) *tenant.Service {
	t.Helper()

	logger := zaptest.NewLogger(t)
	store := inmem.NewKVStore()
	if err := all.Up(context.Background(), logger, store); err != nil {
		t.Fatal(err)
	}

	return tenant.NewService(tenant.NewStore(store))
}
