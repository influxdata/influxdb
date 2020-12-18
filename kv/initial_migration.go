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
		[]byte("authorizationsv1"),
		[]byte("authorizationindexv1"),
		[]byte("bucketsv1"),
		[]byte("bucketindexv1"),
		[]byte("dashboardsv2"),
		[]byte("orgsdashboardsv1"),
		[]byte("dashboardcellviewsv1"),
		kvlogBucket,
		kvlogIndex,
		[]byte("labelsv1"),
		[]byte("labelmappingsv1"),
		[]byte("labelindexv1"),
		[]byte("onboardingv1"),
		[]byte("organizationsv1"),
		[]byte("organizationindexv1"),
		taskBucket,
		taskRunBucket,
		taskIndexBucket,
		[]byte("userspasswordv1"),
		scrapersBucket,
		[]byte("secretsv1"),
		[]byte("telegrafv1"),
		[]byte("telegrafPluginsv1"),
		[]byte("userresourcemappingsv1"),
		[]byte("notificationRulev1"),
		[]byte("usersv1"),
		[]byte("userindexv1"),
		sourceBucket,
		// these are the "document" (aka templates) key prefixes
		[]byte("templates/documents/content"),
		[]byte("templates/documents/meta"),
		// store base backed services
		[]byte("checksv1"),
		[]byte("checkindexv1"),
		[]byte("notificationEndpointv1"),
		[]byte("notificationEndpointIndexv1"),
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
