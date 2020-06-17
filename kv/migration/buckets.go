package migration

import (
	"context"

	"github.com/influxdata/influxdb/v2/kv"
)

// CreateBucketsMigration is a migration Spec which creates
// the provided list of buckets on a store when Up is called
// and deletes them on Down.
type CreateBucketsMigration struct {
	name    string
	buckets [][]byte
}

// CreateBuckets returns a new CreateBucketsMigration Spec.
func CreateBuckets(name string, buckets ...[]byte) Spec {
	return CreateBucketsMigration{name, buckets}
}

// MigrationName returns the name of the migration.
func (c CreateBucketsMigration) MigrationName() string {
	return c.name
}

// Up creates the buckets on the store.
func (c CreateBucketsMigration) Up(ctx context.Context, store kv.SchemaStore) error {
	for _, bucket := range c.buckets {
		if err := store.CreateBucket(ctx, bucket); err != nil {
			return err
		}
	}

	return nil
}

// Down delets the buckets on the store.
func (c CreateBucketsMigration) Down(ctx context.Context, store kv.SchemaStore) error {
	for _, bucket := range c.buckets {
		if err := store.DeleteBucket(ctx, bucket); err != nil {
			return err
		}
	}

	return nil
}
