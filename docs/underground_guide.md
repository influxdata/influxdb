# Underground Guide to testing IOx locally

This document explains how to run IOx locally (for locally
profiling, for example) similarly to how it is deployed in production
but from source in your local development environment where you can
run low key experiments.

This is an "underground" guide in the sense that it is not meant to
define an official setup for profiling or benchmarking and should not
be used for such. It is provided in the hope it will be helpful for
developers.

## Step 1: Build IOx

Build IOx for release with pprof:

```shell
cd influxdb_iox
cargo build --release --features=pprof
```

You can also install the `influxdb_iox` command locally via

```shell
cd influxdb_iox
cargo install --path influxdb_iox
```

## Step 2: Start postgres

Now, start up postgres locally in a docker container:
```shell
# Run postgres
docker run -p 5432:5432 -e POSTGRES_HOST_AUTH_METHOD=trust postgres &
```

Of course, you can also use locally running services (if, for example,
you have postgres running locally on port 5432).


# Step 3: Do one time initialization setup


```shell
# initialize the catalog
INFLUXDB_IOX_CATALOG_DSN=postgres://postgres@localhost:5432/postgres \
OBJECT_STORE=file \
DATABASE_DIRECTORY=~/data_dir \
LOG_FILTER=debug \
./target/release/influxdb_iox catalog setup
```

## Inspecting Catalog state

Depending on what you are trying to do, you may want to inspect the
catalog.

You can run psql like this to inspect the catalog:
```shell
psql -h localhost -p 5432 -U postgres
```

```sql
postgres=# set search_path = iox_catalog;
SET
postgres=# \d
                         List of relations
   Schema    |             Name              |   Type   |  Owner
-------------+-------------------------------+----------+----------
 iox_catalog | _sqlx_migrations              | table    | postgres
 iox_catalog | column_name                   | table    | postgres
 iox_catalog | column_name_id_seq            | sequence | postgres
 iox_catalog | kafka_topic                   | table    | postgres
 iox_catalog | kafka_topic_id_seq            | sequence | postgres
 iox_catalog | namespace                     | table    | postgres
 iox_catalog | namespace_id_seq              | sequence | postgres
 iox_catalog | parquet_file                  | table    | postgres
 iox_catalog | parquet_file_id_seq           | sequence | postgres
 iox_catalog | partition                     | table    | postgres
 iox_catalog | partition_id_seq              | sequence | postgres
 iox_catalog | query_pool                    | table    | postgres
 iox_catalog | query_pool_id_seq             | sequence | postgres
 iox_catalog | sequencer                     | table    | postgres
 iox_catalog | sequencer_id_seq              | sequence | postgres
 iox_catalog | sharding_rule_override        | table    | postgres
 iox_catalog | sharding_rule_override_id_seq | sequence | postgres
 iox_catalog | table_name                    | table    | postgres
 iox_catalog | table_name_id_seq             | sequence | postgres
 iox_catalog | tombstone                     | table    | postgres
 iox_catalog | tombstone_id_seq              | sequence | postgres
(22 rows)

postgres=#
```

# Step 4: Run the services

## Run Ingester on port 8083/8083 (http/grpc)

```shell
INFLUXDB_IOX_BIND_ADDR=localhost:8083 \
INFLUXDB_IOX_GRPC_BIND_ADDR=localhost:8084 \
INFLUXDB_IOX_PAUSE_INGEST_SIZE_BYTES=5000000000 \
INFLUXDB_IOX_PERSIST_MEMORY_THRESHOLD_BYTES=4000000000 \
INFLUXDB_IOX_CATALOG_DSN=postgres://postgres@localhost:5432/postgres \
INFLUXDB_IOX_MAX_HTTP_REQUEST_SIZE=100000000 \
OBJECT_STORE=file \
DATABASE_DIRECTORY=~/data_dir \
LOG_FILTER=info \
./target/release/influxdb_iox run ingester
```

## Run Router on port 8080/8081 (http/grpc)

```shell
INFLUXDB_IOX_BIND_ADDR=localhost:8080 \
INFLUXDB_IOX_GRPC_BIND_ADDR=localhost:8081 \
INFLUXDB_IOX_CATALOG_DSN=postgres://postgres@localhost:5432/postgres \
OBJECT_STORE=file \
DATABASE_DIRECTORY=~/data_dir \
LOG_FILTER=info \
./target/release/influxdb_iox run router
```

# Step 5: Ingest data

You can load data using the influxdb_iox client:
```shell
influxdb_iox --host=http://localhost:8080 -v write test_db test_fixtures/lineproto/*.lp
```

Now you can post data to `http://localhost:8080` with your favorite load generating tool

My favorite is https://github.com/alamb/low_card

To run:
```shell
git clone git@github.com:alamb/low_card.git
cd low_card
cargo run --release
```

Then tweak the parameters in `main.rs` code to change the shape of the
data. The default settings at the time of this writing would result in
posting fairly large requests (necessitating the
`INFLUXDB_IOX_MAX_HTTP_REQUEST_SIZE` setting above)

# Step 6: Profile

See [`profiling.md`](./profiling.md).

# Step 7: Clean up local state

If you find yourself needing to clean up postgres state, use this command:

```shell
docker ps -a -q | xargs docker stop
```
