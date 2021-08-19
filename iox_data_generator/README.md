# `iox_data_generator`

The `iox_data_generator` tool creates random data points according to a specification and loads them
into an `iox` instance to simulate real data.

To build and run, [first install Rust](https://www.rust-lang.org/tools/install). Then from root of the `influxdb_iox` repo run:

```
cargo build --release
```

And the built binary has command line help:

```
./target/release/data_generator --help
```

For examples of specifications see the [schemas folder](schemas)

## Use with two IOx servers and Kafka

The data generator tool can be used to simulate data being written to IOx in various shapes. This
is how to set up a local experiment for profiling or debugging purposes using a database in two IOx
instances: one writing to Kafka and one reading from Kafka.

If you're profiling IOx, be sure you've compiled and are running a release build using either:

```
cargo build --release
./target/release/influxdb_iox run --server-id 1
```

or:

```
cargo run --release -- run --server-id 1
```

Server ID is the only required attribute for running IOx; see `influxdb_iox run --help` for all the
other configuration options for the server you may want to set for your experiment. Note that the
default HTTP API address is `127.0.0.1:8080` unless you set something different with `--api-bind`
and the default gRPC address is `127.0.0.1:8082` unless you set something different using
`--grpc-bind`.

For the Kafka setup, you'll need to start two IOx servers, so you'll need to set the bind addresses
for at least one of them. Here's an example of the two commands to run:

```
cargo run --release -- run --server-id 1
cargo run --release -- run --server-id 2 --api-bind 127.0.0.1:8084 --grpc-bind 127.0.0.1:8086
```

You'll also need to run a Kafka instance. There's a Docker compose script in the influxdb_iox
repo you can run with:

```
docker-compose -f docker/ci-kafka-docker-compose.yml up kafka
```

The Kafka instance will be accessible from `127.0.0.1:9093` if you run it with this script.

Once you have the two IOx servers and one Kafka instance running, create a database with a name in
the format `[orgname]_[bucketname]`. For example, create a database in IOx named `mlb_pirates`, and
the org you'll use in the data generator will be `mlb` and the bucket will be `pirates`. The
`DatabaseRules` defined in `src/bin/create_database.rs` will set up a database in the "writer" IOx
instance to write to Kafka and the database in the "reader" IOx instance to read from Kafka if
you run it with:

```
cargo run --bin create_database -- --writer 127.0.0.1:8082 --reader 127.0.0.1:8086 mlb_pirates
```

This script adds 3 rows to a `writer_test` table because [this issue with the Kafka Consumer
needing data before it can find partitions](https://github.com/influxdata/influxdb_iox/issues/2189).

Once the database is created, decide what kind of data you would like to send it. You can use an
existing data generation schema in the `schemas` directory or create a new one, perhaps starting
from an existing schema as a guide. In this example, we're going to use `schemas/cap-write.toml`.

Next, run the data generation tool as follows:

```
# in your iox_data_generator checkout
cargo run -- --spec iox_data_generator/schemas/cap-write.toml --continue --host 127.0.0.1:8080 --token arbitrary --org mlb --bucket pirates
```

- `--spec schemas/cap-write.toml` sets the schema you want to use to generate the data
- `--continue` means the data generation tool should generate data every `sampling_interval` (which
  is set in the schema) until we stop it
- `--host 127.0.0.1:8080` means to write to the writer IOx server running at the default HTTP API address
  of `127.0.0.1:8080` (note this is NOT the gRPC address used by the `create_database` command)
- `--token arbitrary` - the data_generator requires a token value but IOx doesn't use it, so this
  can be any value.
- `--org mlb` is the part of the database name you created before the `_`
- `--bucket pirates` is the part of the database name you created after the `_`

You should be able to use `influxdb_iox sql -h http://127.0.0.1:8086` to connect to the gRPC of the reader
then `use database mlb_pirates;` and query the tables to see that the data is being inserted. That
is,

```
# in your influxdb_iox checkout
cargo run -- sql -h http://127.0.0.1:8086
```

Connecting to the writer instance won't show any data.
