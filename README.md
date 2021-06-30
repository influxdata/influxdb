# InfluxDB IOx

InfluxDB IOx (short for Iron Oxide, pronounced InfluxDB "eye-ox") is the future core of InfluxDB, an open source time series database.
The name is in homage to Rust, the language this project is written in.
It is built using [Apache Arrow](https://arrow.apache.org/) and [DataFusion](https://arrow.apache.org/blog/2019/02/04/datafusion-donation/) among other things.
InfluxDB IOx aims to be:

* The future core of InfluxDB; supporting industry standard SQL, InfluxQL, and Flux
* An in-memory columnar store using object storage for persistence
* A fast analytic database for structured and semi-structured events (like logs and tracing data)
* A system for defining replication (synchronous, asynchronous, push and pull) and partitioning rules for InfluxDB time series data and tabular analytics data
* A system supporting real-time subscriptions
* A processor that can transform and do arbitrary computation on time series and event data as it arrives
* An analytic database built for data science, supporting Apache Arrow Flight for fast data transfer

Persistence is through Parquet files in object storage.
It is a design goal to support integration with other big data systems through object storage and Parquet specifically.

For more details on the motivation behind the project and some of our goals, read through the [InfluxDB IOx announcement blog post](https://www.influxdata.com/blog/announcing-influxdb-iox/).
If you prefer a video that covers a little bit of InfluxDB history and high level goals for [InfluxDB IOx you can watch Paul Dix's announcement talk from InfluxDays NA 2020](https://www.youtube.com/watch?v=pnwkAAyMp18).
For more details on the motivation behind the selection of [Apache Arrow, Flight and Parquet, read this](https://www.influxdata.com/blog/apache-arrow-parquet-flight-and-their-ecosystem-are-a-game-changer-for-olap/).

## Project Status

This project is very early and in active development. It isn't yet ready for testing, which is why we're not producing builds or documentation yet. If you're interested in following along with the project, drop into our community Slack channel #influxdb_iox. You can find [links to join here](https://community.influxdata.com/).

We're also hosting monthly tech talks and community office hours on the project on the 2nd Wednesday of the month at 8:30 AM Pacific Time.
* [Signup for upcoming IOx tech talks](https://www.influxdata.com/community-showcase/influxdb-tech-talks)
* [Watch past IOx tech talks](https://www.youtube.com/playlist?list=PLYt2jfZorkDp-PKBS05kf2Yx2NrRyPAAz)

## Get started

1. [Install dependencies](#install-dependencies)
1. [Clone the repository](#clone-the-repository)
1. [Configure the server](#configure-the-server)
1. [Compile and start the server](#compile-and-start-the-server)  
   (You can also [build a Docker image](#build-a-docker-image-optional) to run InfluxDB IOx.)
1. [Write and read data](#write-and-read-data)
1. [Use the CLI](#use-the-cli)
1. [Use InfluxDB 2.0 API compatibility](#use-influxdb-20-api-compatibility)
1. [Run health checks](#run-health-checks)
1. [Manually call the gRPC API](#manually-call-the-grpc-api)

### Install dependencies

To compile and run InfluxDB IOx from source, you'll need the following:

- [Rust](#rust)
- [Clang](#clang)

#### Rust

The easiest way to install Rust is to use [`rustup`](https://rustup.rs/), a Rust version manager.
Follow the instructions for your operating system on the `rustup` site.

`rustup` will check the [`rust-toolchain`](./rust-toolchain.toml) file and automatically install and use the correct Rust version for you.

#### Clang

Building InfluxDB IOx requires `clang` (for the [`croaring`] dependency).
Check for `clang` by running `clang --version`.

```shell
clang --version
Apple clang version 12.0.0 (clang-1200.0.32.27)
Target: x86_64-apple-darwin20.1.0
Thread model: posix
InstalledDir: /Library/Developer/CommandLineTools/usr/bin
```

If `clang` is not already present, it can typically be installed with the system package manager.

[`croaring`]: https://github.com/saulius/croaring-rs

### Clone the repository

Clone this repository using `git`.
If you use the `git` command line, this looks like:

```shell
git clone git@github.com:influxdata/influxdb_iox.git
```

Then change into the directory containing the code:

```shell
cd influxdb_iox
```

The rest of these instructions assume you are in this directory.

### Configure the server

InfluxDB IOx can be configured using either environment variables or a configutation file,
making it suitable for deployment in containerized environments.

For a list configuration options, run `influxdb_iox --help`.
For configuration options for specific subcommands, run `influxdb_iox <subcommand> --help`.

To use a configuration file, use a `.env` file in the working directory.
See the provided [example configuration file](docs/env.example).
To use the example configuration file, run:

```shell
cp docs/env.example .env
```

### Compile and start the server

InfluxDB IOx is built using Cargo, Rust's package manager and build tool.

To compile for development, run:

```shell
cargo build
```

This which will create a binary at `target/debug/influxdb_iox`.

To start the InfluxDB IOx server, run:

```shell
./target/debug/influxdb_iox run
```

By default the server will start an HTTP server on port `8080` and a gRPC server on port `8082`.

You can also compile and run with one command:

```shell
cargo run -- run
```

To compile for performance testing, build in release mode:

```shell
cargo build --release
./target/release/influxdb_iox run
```

You can also run in release mode with one step:

```shell
cargo run --release -- run
```

### Build a Docker image (optional)

Building the Docker image requires:

- Docker 18.09+
- BuildKit

To [enable BuildKit] by default, set `{ "features": { "buildkit": true } }` in the Docker engine configuration,
or run `docker build` with`DOCKER_BUILDKIT=1`

To build the Docker image:

```
DOCKER_BUILDKIT=1 docker build .
```

[Enable BuildKit]: https://docs.docker.com/develop/develop-images/build_enhancements/#to-enable-buildkit-builds
### Write and read data

Each IOx instance requires a writer ID.
This can be set one of 4 ways:
- set an environment variable `INFLUXDB_IOX_ID=42`
- set a flag `--writer-id 42`
- use the API (not convered here)
- use the CLI
  ```shell
  influxdb_iox writer set 42
  ```

To write data, you need to create a database.
You can do so via the API or using the CLI.
For example, to create a database called `company_sensors`, use this command:

```shell
influxdb_iox database create company_sensors
```

Data can be stored in InfluxDB IOx by sending it in [line protocol] format to the `/api/v2/write` endpoint or using the CLI.
For example, here is a command that will send the data in the `tests/fixtures/lineproto/metrics.lp` file in this repository, assuming that you're running the server on the default port into the `company_sensors` database, you can use:

```shell
influxdb_iox database write company_sensors tests/fixtures/lineproto/metrics.lp
```

To query data stored in the `company_sensors` database:

```shell
influxdb_iox database query company_sensors "SELECT * FROM cpu LIMIT 10"
```

### Use the CLI

InfluxDB IOx is packaged as a binary with commands to start the IOx server,
as well as a CLI interface for interacting with and configuring such servers.

The CLI itself is documented via built-in help which you can access by running `influxdb_iox --help`

### Use InfluxDB 2.0 API compatibility

InfluxDB IOx allows seamless interoperability with InfluxDB 2.0.

Where InfluxDB 2.0 stores data in organizations and buckets,
InfluxDB IOx stores data in _named databases_.
IOx maps `organization` and `bucket` pairs to databases named with the two parts separated by an underscore (`_`):
`organization_bucket`.

Here's an example using [`curl`] to send data into the `company_sensors` database using the InfluxDB 2.0 `/api/v2/write` API:

```shell
curl -v "http://127.0.0.1:8080/api/v2/write?org=company&bucket=sensors" --data-binary @tests/fixtures/lineproto/metrics.lp
```

[line protocol]: https://docs.influxdata.com/influxdb/v2.0/reference/syntax/line-protocol/
[`curl`]: https://curl.se/

### Run health checks

The HTTP API exposes a healthcheck endpoint at `/health`

```console
$ curl http://127.0.0.1:8080/health
OK
```

The gRPC API implements the [gRPC Health Checking Protocol](https://github.com/grpc/grpc/blob/master/doc/health-checking.md).
This can be tested with [`grpc-health-probe`](https://github.com/grpc-ecosystem/grpc-health-probe):

```console
$ grpc_health_probe -addr 127.0.0.1:8082 -service influxdata.platform.storage.Storage
status: SERVING
```

### Manually call the gRPC API

To manually invoke one of the gRPC APIs, use a gRPC CLI client such as [grpcurl](https://github.com/fullstorydev/grpcurl).

Tonic (the gRPC server library we're using) currently doesn't have support for gRPC reflection, hence you must pass all `.proto` files to your client.
You can find a conventient `grpcurl` wrapper that does that in the `scripts` directory:

```console
$ ./scripts/grpcurl -plaintext 127.0.0.1:8082 list
grpc.health.v1.Health
influxdata.iox.management.v1.ManagementService
influxdata.platform.storage.IOxTesting
influxdata.platform.storage.Storage
$ ./scripts/grpcurl -plaintext 127.0.0.1:8082 influxdata.iox.management.v1.ManagementService.ListDatabases
{
  "names": [
    "foobar_weather"
  ]
}
```

## Contributing

We welcome community contributions from anyone!

Read our [Contributing Guide](CONTRIBUTING.md) for instructions on how to run tests and how to make your first contribution.

## Architecture and Technical Documenation

There are a variety of technical documents describing various parts of IOx in the [docs](docs) directory.
