// package migration
//
// This package contains utility types for composing and running schema and data migrations
// in a strictly serial and ordered nature; against a backing kv.SchemaStore implementation.
//
// The goal is provide a mechanism to ensure an ordered set of changes are applied once
// and only once to a persisted kv store. To ensure we can make guarantees from one migration
// to the next, based on the mutations of the previous migrations.
//
// The package offers the `Migrator` type which takes a slice of `Spec` implementations.
// A spec is a single migration definition, which exposes a name, up and down operations
// expressed as an Up and Down function on the Spec implementation.
//
// The `Migrator` on a call to `Up(ctx)` applies these defined list of migrations respective `Up(...)` functions
// on a `kv.SchemaStore` in order and persists their invocation on the store in a reserved Bucket `migrationsv1`.
// This is to ensure the only once invocation of the migration takes place and allows to the resuming or introduction
// of new migrations at a later date.
// This means the defined list needs to remain static from the point of application. Otherwise an error will be raised.
//
// This package also offer utilities types for quickly defining common changes as specifications.
// For example creating buckets, when can be quickly constructed via `migration.CreateBuckets("create buckets ...", []byte("foo"), []byte{"bar"})`.
//
// As of today all migrations be found in a single defintion in the sub-package to this one
// named `all` (see `kv/migration/all/all.go`).
// The `migration.CreateNewMigration()` method can be used to manipulate this `all.go` file in the package and quickly
// add a new migration file to be populated. This is accessible on the command line via the `internal/cmd/kvmigrate` buildable go tool.
// Try `go run internal/cmd/kvmigrate/main.go`.
package migration
