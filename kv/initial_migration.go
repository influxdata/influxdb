package kv

import (
	"context"
	"encoding/json"

	influxdb "github.com/influxdata/influxdb/v2"
)

type InitialMigration struct{}

// MigrationName returns the string initial migration
// which allows this store to be used as a migration
func (m InitialMigration) MigrationName() string {
	return "initial migration"
}

// Up initializes all the owned buckets of the underlying store
func (m InitialMigration) Up(ctx context.Context, store SchemaStore) error {
	// please do not initialize anymore buckets here
	// add them as a new migration to the list of migrations
	// defined in NewInitialMigration.

	for _, bucket := range [][]byte{
		authBucket,
		authIndex,
		bucketBucket,
		bucketIndex,
		dashboardBucket,
		orgDashboardIndex,
		dashboardCellViewBucket,
		kvlogBucket,
		kvlogIndex,
		labelBucket,
		labelMappingBucket,
		labelIndex,
		onboardingBucket,
		organizationBucket,
		organizationIndex,
		taskBucket,
		taskRunBucket,
		taskIndexBucket,
		userpasswordBucket,
		scrapersBucket,
		secretBucket,
		telegrafBucket,
		telegrafPluginsBucket,
		urmBucket,
		notificationRuleBucket,
		userBucket,
		userIndex,
		sourceBucket,
		// these are the "document" (aka templates) key prefixes
		[]byte("templates/documents/content"),
		[]byte("templates/documents/meta"),
		// store base backed services
		checkBucket,
		checkIndexBucket,
		notificationEndpointBucket,
		notificationEndpointIndexBucket,
		variableBucket,
		variableIndexBucket,
		variableOrgsIndex,
		// deprecated: removed in later migration
		[]byte("sessionsv1"),
	} {
		if err := store.CreateBucket(ctx, bucket); err != nil {
			return err
		}
	}

	// seed initial sources (default source)
	return store.Update(ctx, func(tx Tx) error {
		return putAsJson(tx, sourceBucket, DefaultSource.ID, DefaultSource)
	})
}

// Down is a no operation required for service to be used as a migration
func (m InitialMigration) Down(ctx context.Context, store SchemaStore) error {
	return nil
}

func putAsJson(tx Tx, bucket []byte, id influxdb.ID, value interface{}) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}

	encodedID, err := id.Encode()
	if err != nil {
		return err
	}

	b, err := tx.Bucket(bucket)
	if err != nil {
		return err
	}

	return b.Put(encodedID, data)
}
