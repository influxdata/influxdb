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

func initBucketService(s kv.SchemaStore, f influxdbtesting.BucketFields, t *testing.T) (influxdb.BucketService, string, func()) {
	ctx := context.Background()
	svc := kv.NewService(zaptest.NewLogger(t), s)
	svc.OrgIDs = f.OrgIDs
	svc.BucketIDs = f.BucketIDs
	svc.IDGenerator = f.IDGenerator
	svc.TimeGenerator = f.TimeGenerator
	if f.TimeGenerator == nil {
		svc.TimeGenerator = influxdb.RealTimeGenerator{}
	}

	for _, o := range f.Organizations {
		// new tenant services do this properly
		o.ID = svc.OrgIDs.ID()
		if err := svc.PutOrganization(ctx, o); err != nil {
			t.Fatalf("failed to populate organizations: %s", err)
		}
	}

	for _, b := range f.Buckets {
		// new tenant services do this properly
		b.ID = svc.BucketIDs.ID()
		if err := svc.PutBucket(ctx, b); err != nil {
			t.Fatalf("failed to populate buckets: %s", err)
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
