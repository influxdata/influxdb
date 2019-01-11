package storage_test

import (
	"context"
	"testing"

	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/inmem"
	"github.com/influxdata/influxdb/storage"
)

func TestBucketService(t *testing.T) {
	service := storage.NewBucketService(nil, nil)

	i, err := platform.IDFromString("2222222222222222")
	if err != nil {
		panic(err)
	}

	if err := service.DeleteBucket(context.TODO(), *i); err == nil {
		t.Fatal("expected error, got nil")
	}

	inmemService := inmem.NewService()
	service = storage.NewBucketService(inmemService, nil)

	if err := service.DeleteBucket(context.TODO(), *i); err == nil {
		t.Fatal("expected error, got nil")
	}

	org := &platform.Organization{}
	if err := inmemService.CreateOrganization(context.TODO(), org); err != nil {
		panic(err)
	}

	bucket := &platform.Bucket{OrganizationID: org.ID}
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
	orgID, bucketID platform.ID
}

func (m *MockDeleter) DeleteBucket(orgID, bucketID platform.ID) error {
	m.orgID, m.bucketID = orgID, bucketID
	return nil
}
