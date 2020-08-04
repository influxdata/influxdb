package storage_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/inmem"
	"github.com/influxdata/influxdb/v2/kv/migration/all"
	"github.com/influxdata/influxdb/v2/storage"
	"github.com/influxdata/influxdb/v2/tenant"
	"go.uber.org/zap/zaptest"
)

func TestFindBuckets(t *testing.T) {
	s := inmem.NewKVStore()
	ctx := context.Background()
	if err := all.Up(ctx, zaptest.NewLogger(t), s); err != nil {
		t.Fatal(err)
	}

	st := tenant.NewStore(s)
	tenant := tenant.NewService(st)
	id := influxdb.ID(1)
	err := tenant.CreateOrganization(ctx, &influxdb.Organization{Name: "default", ID: id})
	if err != nil {
		t.Fatal(err)
	}

	expected := 25
	for i := 0; i < expected; i++ {
		tenant.CreateBucket(ctx, &influxdb.Bucket{Name: fmt.Sprintf("bucket%d", i), OrgID: id})
	}

	finder := storage.NewAllBucketsFinder(tenant)
	_, n, err := finder.FindBuckets(ctx, influxdb.BucketFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if n != expected+2 { // add two for the system buckets
		t.Fatalf("expected %d buckets, got: %d", expected, n)
	}
}
