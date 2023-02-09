// package all
//
// This package is the canonical location for all migrations being made against the
// single shared kv.Store implementation used by InfluxDB (while it remains a single store).
//
// The array all.Migrations contains the list of migration specifications which drives the
// serial set of migration operations required to correctly configure the backing metadata store
// for InfluxDB.
//
// This package is arranged like so:
//
//	doc.go - this piece of documentation.
//	all.go - definition of Migration array referencing each of the name migrations in number migration files (below).
//	migration.go - an implementation of migration.Spec for convenience.
//	000X_migration_name.go (example) - N files contains the specific implementations of each migration enumerated in `all.go`.
//	...
//
// Managing this list of files and all.go can be fiddly.
// There is a buildable cli utility called `kvmigrate` in the `internal/cmd/kvmigrate` package.
// This has a command `create` which automatically creates a new migration in the expected location
// and appends it appropriately into the all.go Migration array.
package all
