# IOx Catalog

This crate contains the code for the IOx Catalog. This includes the definitions of namespaces,
their tables, the columns of those tables and their types, what Parquet files are in object storage
and delete tombstones. There's also some configuration information that the overall distributed
system uses for operation.

To run this crate's tests you'll need Postgres installed and running locally. You'll also need to
set the `INFLUXDB_IOX_CATALOG_DSN` environment variable so that sqlx will be able to connect to
your local DB. For example with user and password filled in:

```
INFLUXDB_IOX_CATALOG_DSN=postgres://<postgres user>:<postgres password>@localhost/iox_shared
```

You can omit the host part if your postgres is running on the default unix domain socket (useful on
macos because, by default, the config installed by `brew install postgres` doesn't listen to a TCP
port):

```
INFLUXDB_IOX_CATALOG_DSN=postgres:///iox_shared
```

You'll then need to create the database. You can do this via the sqlx command line.

```
cargo install sqlx-cli
DATABASE_URL=<dsn> sqlx database create
cargo run -q -- catalog setup
```

This will set up the database based on the files in `./migrations` in this crate. SQLx also creates
a table to keep track of which migrations have been run.

NOTE: **do not** use `sqlx database setup`, because that will create the migration table in the
wrong schema (namespace). Our `catalog setup` code will do that part by using the same sqlx
migration module but with the right namespace setup.

## Migrations

If you need to create and run migrations to add, remove, or change the schema, you'll need the
`sqlx-cli` tool. Install with `cargo install sqlx-cli` if you haven't already, then run `sqlx
migrate --help` to see the commands relevant to migrations.

## Tests

To run the Postgres integration tests, ensure the above setup is complete first.

**CAUTION:** existing data in the database is dropped when tests are run, so you should use a
DIFFERENT database name for your test database than your `INFLUXDB_IOX_CATALOG_DSN` database.

* Set `TEST_INFLUXDB_IOX_CATALOG_DSN=<testdsn>` env as above with the `INFLUXDB_IOX_CATALOG_DSN`
  env var. The integration tests *will* pick up this value if set in your `.env` file.
* Set `TEST_INTEGRATION=1`
* Run `cargo test -p iox_catalog`

## Schema namespace

All iox catalog tables are created in a `iox_catalog` schema. Remember to set the schema search
path when accessing the database with `psql`.

There are several ways to set the default search path, depending if you want to do it for your
session, for the database or for the user.

Setting a default search path for the database or user may interfere with tests (e.g. it may make
some test pass when they should fail). The safest option is set the search path on a per session
basis. As always, there are a few ways to do that:

1. you can type `set search_path to public,iox_catalog;` inside psql.
2. you can add (1) to your `~/.psqlrc`
3. or you can just pass it as a CLI argument with:

```
psql 'dbname=iox_shared options=-csearch_path=public,iox_catalog'
```
