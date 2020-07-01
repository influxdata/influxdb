package all

import (
	"github.com/influxdata/influxdb/v2/kv/migration"
)

// Migrations contains all the migrations required for the entire of the
// kv store backing influxdb's metadata.
var Migrations = [...]migration.Spec{
	// initial migrations
	Migration0001_InitialMigration,
	// add index user resource mappings by user id
	Migration0002_AddURMByUserIndex,
	// add index for tasks with missing owner IDs
	Migration0003_TaskOwnerIDUpMigration,
	// add dbrp buckets
	Migration0004_AddDbrpBuckets,
	// add pkger buckets
	Migration0005_AddPkgerBuckets,
	// {{ do_not_edit . }}
}
