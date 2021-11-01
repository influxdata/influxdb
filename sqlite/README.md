## SQlite

### Purpose

This `sqlite` package provides a basic interface for interacting with the
embedded sqlite database used by various InfluxDB services which require storing
relational data.

The actual sqlite driver is provided by
[`mattn/go-sqlite3`](https://github.com/mattn/go-sqlite3).

### Usage

A single instance of `SqlStore` should be created using the `NewSqlStore`
function. Currently, this is done in the top-level `launcher` package, and a
pointer to the `SqlStore` instance is passed to services which require it as
part of their initialization.

The [`jmoiron/sqlx`](https://github.com/jmoiron/sqlx) package provides a
convenient and lightweight means to write and read structs into and out of the
database and is sufficient for performing simple, static queries. For more
complicated & dynamically constructed queries, the
[`Masterminds/squirrel`](https://github.com/Masterminds/squirrel) package can be
used as a query builder.

### Concurrent Access

An interesting aspect of using the file-based sqlite database is that while it
can support multiple concurrent read requests, only a single write request can
be processed at a time. A traditional RDBMS would manage concurrent write
requests on the database server, but for this sqlite implementation write
requests need to be managed in the application code.

In practice, this means that code intended to mutate the database needs to
obtain a write lock prior to making queries that would result in a change to the
data. If locks are not obtained in the application code, it is possible that
errors will be encountered if concurrent write requests hit the database file at
the same time.

### Migrations

A simple migration system is implemented in `migrator.go`. When starting the
influx daemon, the migrator runs migrations defined in `.sql` files using
sqlite-compatible sql scripts. Records of these migrations are maintained in a
table called "migrations". If records of migrations exist in the "migrations"
table that are not embedded in the binary, an error will be raised on startup.

When creating new migrations, follow the file naming convention established by
existing migration scripts, which should look like `00XX_script_name.up.sql` &
`00xx_script_name.down.sql` for the "up" and "down" migration, where `XX` is the
version number. New scripts should have the version number incremented by 1.

The "up" migrations are run when starting the influx daemon and when metadata
backups are restored. The "down" migrations are run with the `influxd downgrade`
command.

### In-Memory Database

When running `influxd` with the `--store=memory` flag, the database will be
opened using the `:memory:` path, and the maximum number of open database
connections is set to 1. Because of the way in-memory databases work with
sqlite, each connection would see a completely new database, so using only a
single connection will ensure that requests to `influxd` will return a
consistent set of data.

### Backup & Restore

Methods for backing up and restoring the sqlite database are available on the
`SqlStore` struct. These operations make use of the [sqlite backup
API](https://www.sqlite.org/backup.html) made available by the `go-sqlite3`
driver. It is possible to restore and backup into sqlite databases either stored
in memory or on disk.

### Sqlite Features / Extensions

There are many additional features and extensions available, see [the go-sqlite3
package docs](https://github.com/mattn/go-sqlite3#feature--extension-list) for
the full list.

We currently use the `sqlite_foreign_keys` and `sqlite_json` extensions for
foreign key support & JSON query support. These features are enabled using
build tags defined in the `Makefile` and `.goreleaser` config for use in
local builds & CI builds respectively.
