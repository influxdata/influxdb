package migration

import (
	"context"
	"errors"
	"testing"

	"github.com/influxdata/influxdb/v2/inmem"
	"github.com/influxdata/influxdb/v2/kv"
)

func Test_BucketsMigration_CreateBuckets(t *testing.T) {
	var (
		ctx    = context.Background()
		bucket = []byte("some_bucket")
		store  = inmem.NewKVStore()
	)

	// bucket should not exist
	bucketShouldNotExist(t, store, bucket)

	// build new create buckets migration
	migration := CreateBuckets("create bucket some_bucket", bucket)

	// apply migration up
	if err := migration.Up(ctx, store); err != nil {
		t.Fatal("unexpected error", err)
	}

	// bucket should now exist
	bucketShouldExist(t, store, bucket)

	// apply migration down
	if err := migration.Down(ctx, store); err != nil {
		t.Fatal("unexpected error", err)
	}

	// bucket should no longer exist
	bucketShouldNotExist(t, store, bucket)
}

func Test_BucketsMigration_DeleteBuckets(t *testing.T) {
	var (
		ctx    = context.Background()
		bucket = []byte("some_bucket")
		store  = inmem.NewKVStore()
	)

	// initially create bucket
	if err := store.CreateBucket(ctx, bucket); err != nil {
		t.Fatal("unexpected error", err)
	}

	// ensure bucket is there to start with
	bucketShouldExist(t, store, bucket)

	// build new delete buckets migration
	migration := DeleteBuckets("delete bucket some_bucket", bucket)

	// apply migration up
	if err := migration.Up(ctx, store); err != nil {
		t.Fatal("unexpected error", err)
	}

	// bucket should have been removed
	bucketShouldNotExist(t, store, bucket)

	// apply migration down
	if err := migration.Down(ctx, store); err != nil {
		t.Fatal("unexpected error", err)
	}

	// bucket should exist again
	bucketShouldExist(t, store, bucket)
}

func bucketShouldExist(t *testing.T, store kv.Store, bucket []byte) {
	t.Helper()

	if err := store.View(context.Background(), func(tx kv.Tx) error {
		_, err := tx.Bucket(bucket)
		return err
	}); err != nil {
		t.Fatal("unexpected error", err)
	}
}

func bucketShouldNotExist(t *testing.T, store kv.Store, bucket []byte) {
	t.Helper()

	if err := store.View(context.Background(), func(tx kv.Tx) error {
		_, err := tx.Bucket(bucket)
		return err
	}); !errors.Is(err, kv.ErrBucketNotFound) {
		t.Fatalf("expected bucket not found, got %q", err)
	}
}
