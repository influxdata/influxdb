package storage_test

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/inmem"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/storage"
	"go.uber.org/zap/zaptest"
)

func TestBucketService(t *testing.T) {
	service := storage.NewBucketService(nil, nil)

	i, err := influxdb.IDFromString("2222222222222222")
	if err != nil {
		panic(err)
	}

	if err := service.DeleteBucket(context.TODO(), *i); err == nil {
		t.Fatal("expected error, got nil")
	}

	inmemService := newInMemKVSVC(t)
	service = storage.NewBucketService(inmemService, nil)

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

	// Test deleting a bucket calls into the deleter.
	deleter := &MockDeleter{}
	service = storage.NewBucketService(inmemService, deleter)

	if err := service.DeleteBucket(context.TODO(), bucket.ID); err != nil {
		t.Fatal(err)
	}

	if deleter.orgID != org.ID {
		t.Errorf("got org ID: %s, expected %s", deleter.orgID, org.ID)
	} else if deleter.bucketID != bucket.ID {
		t.Errorf("got bucket ID: %s, expected %s", deleter.bucketID, bucket.ID)
	}
}

type MockDeleter struct {
	orgID, bucketID influxdb.ID
}

func (m *MockDeleter) DeleteBucket(_ context.Context, orgID, bucketID influxdb.ID) error {
	m.orgID, m.bucketID = orgID, bucketID
	return nil
}

func newInMemKVSVC(t *testing.T) *kv.Service {
	t.Helper()

	svc := kv.NewService(zaptest.NewLogger(t), inmem.NewKVStore())
	if err := svc.Initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	return svc
}
