package kv_test

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kv"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
	"go.uber.org/zap/zaptest"
)

func TestBoltBucketService(t *testing.T) {
	influxdbtesting.BucketService(initBoltBucketService, t)
}

func initBoltBucketService(f influxdbtesting.BucketFields, t *testing.T) (influxdb.BucketService, string, func()) {
	s, closeBolt, err := NewTestBoltStore(t)
	if err != nil {
		t.Fatalf("failed to create new kv store: %v", err)
	}

	svc, op, closeSvc := initBucketService(s, f, t)
	return svc, op, func() {
		closeSvc()
		closeBolt()
	}
}

func initBucketService(s kv.Store, f influxdbtesting.BucketFields, t *testing.T) (influxdb.BucketService, string, func()) {
	svc := kv.NewService(zaptest.NewLogger(t), s)
	svc.OrgBucketIDs = f.OrgBucketIDs
	svc.IDGenerator = f.IDGenerator
	svc.TimeGenerator = f.TimeGenerator
	if f.TimeGenerator == nil {
		svc.TimeGenerator = influxdb.RealTimeGenerator{}
	}

	ctx := context.Background()
	if err := svc.Initialize(ctx); err != nil {
		t.Fatalf("error initializing bucket service: %v", err)
	}
	for _, o := range f.Organizations {
		if err := svc.PutOrganization(ctx, o); err != nil {
			t.Fatalf("failed to populate organizations")
		}
	}
	for _, b := range f.Buckets {
		if err := svc.PutBucket(ctx, b); err != nil {
			t.Fatalf("failed to populate buckets")
		}
	}
	return svc, kv.OpPrefix, func() {
		for _, o := range f.Organizations {
			if err := svc.DeleteOrganization(ctx, o.ID); err != nil {
				t.Logf("failed to remove organization: %v", err)
			}
		}
		for _, b := range f.Buckets {
			if err := svc.DeleteBucket(ctx, b.ID); err != nil {
				t.Logf("failed to remove bucket: %v", err)
			}
		}
	}
}
