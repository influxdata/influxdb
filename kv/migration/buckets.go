package migration

import (
	"context"

	"github.com/influxdata/influxdb/v2/kv"
)

type bucketMigrationType string

const (
	createBucketMigration = bucketMigrationType("create")
	deleteBucketMigration = bucketMigrationType("delete")
)

// BucketsMigration is a migration Spec which creates
// the provided list of buckets on a store when Up is called
// and deletes them on Down.
type BucketsMigration struct {
	typ     bucketMigrationType
	name    string
	buckets [][]byte
}

// CreateBuckets returns a new BucketsMigration Spec.
func CreateBuckets(name string, bucket []byte, extraBuckets ...[]byte) Spec {
	buckets := append([][]byte{bucket}, extraBuckets...)
	return BucketsMigration{createBucketMigration, name, buckets}
}

// DeleteBuckets returns a new BucketsMigration Spec.
func DeleteBuckets(name string, bucket []byte, extraBuckets ...[]byte) Spec {
	buckets := append([][]byte{bucket}, extraBuckets...)
	return BucketsMigration{deleteBucketMigration, name, buckets}
}

// MigrationName returns the name of the migration.
func (m BucketsMigration) MigrationName() string {
	return m.name
}

// Up creates the buckets on the store.
func (m BucketsMigration) Up(ctx context.Context, store kv.SchemaStore) error {
	var fn func(context.Context, []byte) error
	switch m.typ {
	case createBucketMigration:
		fn = store.CreateBucket
	case deleteBucketMigration:
		fn = store.DeleteBucket
	default:
		panic("unrecognized buckets migration type")
	}

	for _, bucket := range m.buckets {
		if err := fn(ctx, bucket); err != nil {
			return err
		}

	}

	return nil
}

// Down delets the buckets on the store.
func (m BucketsMigration) Down(ctx context.Context, store kv.SchemaStore) error {
	var fn func(context.Context, []byte) error
	switch m.typ {
	case createBucketMigration:
		fn = store.DeleteBucket
	case deleteBucketMigration:
		fn = store.CreateBucket
	default:
		panic("unrecognized buckets migration type")
	}

	for _, bucket := range m.buckets {
		if err := fn(ctx, bucket); err != nil {
			return err
		}

	}

	return nil
}
