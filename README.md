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

## Quick Start

To compile and run InfluxDB IOx from source, you'll need a Rust compiler and `clang`.

### Build a Docker Image

**BuildKit is required.**
[Enable BuildKit](https://docs.docker.com/develop/develop-images/build_enhancements/#to-enable-buildkit-builds):
- Use Docker version 18.09 or later
- Enable BuildKit by default by setting `{ "features": { "buildkit": true } }` in the Docker engine config
  - ...or run `docker build .` with env var `DOCKER_BUILDKIT=1`

To build the Docker image:
```
DOCKER_BUILDKIT=1 docker build .
```

### Cloning the Repository

Using `git`, check out the code by cloning this repository. If you use the `git` command line, this
looks like:

```shell
git clone git@github.com:influxdata/influxdb_iox.git
```

Then change into the directory containing the code:

```shell
cd influxdb_iox
```

The rest of the instructions assume you are in this directory.

### Installing Rust

The easiest way to install Rust is by using [`rustup`], a Rust version manager.
Follow the instructions on the `rustup` site for your operating system.

[`rustup`]: https://rustup.rs/

By default, `rustup` will install the latest stable version of Rust. InfluxDB IOx is currently
using a nightly version of Rust to get performance benefits from the unstable `simd` feature. The
exact nightly version is specified in the `rust-toolchain` file. When you're in the directory
containing this repository's code, `rustup` will look in the `rust-toolchain` file and
automatically install and use the correct Rust version for you. Test this out with:

```shell
rustc --version
```

and you should see a nightly version of Rust!

### Installing `clang`

An installation of `clang` is required to build the [`croaring`] dependency - if
it is not already present, it can typically be installed with the system
package manager.

```shell
clang --version
Apple clang version 12.0.0 (clang-1200.0.32.27)
Target: x86_64-apple-darwin20.1.0
Thread model: posix
InstalledDir: /Library/Developer/CommandLineTools/usr/bin
```

[`croaring`]: https://github.com/saulius/croaring-rs

### Specifying Configuration

IOx is designed for running in modern containerized environments. As such, it
takes its configuration as environment variables.

You can see a list of the current configuration values by running `influxdb_iox
--help`, as well as the specific subcommand config options such as `influxdb_iox
run --help`.

Should you desire specifying config via a file, you can do so using a
`.env` formatted file in the working directory. You can use the
provided [example](docs/env.example) as a template if you want:

```shell
cp docs/env.example .env
```

### Compiling and Starting the Server

InfluxDB IOx is built using Cargo, Rust's package manager and build tool.

To compile for development, run:

```shell
cargo build
```

which will create a binary in `target/debug` that you can run with:

```shell
./target/debug/influxdb_iox
```

You can compile and run with one command by using:

```shell
cargo run -- server
```

When compiling for performance testing, build in release mode by using:

```shell
cargo build --release
```

which will create the corresponding binary in `target/release`:

```shell
./target/release/influxdb_iox run
```

Similarly, you can do this in one step with:

```shell
cargo run --release -- server
```

The server will, by default, start an HTTP API server on port `8080` and a gRPC server on port
`8082`.

### Writing and Reading Data

Each IOx instance requires a writer ID.
This can be set one of 4 ways:
- set an environment variable `INFLUXDB_IOX_ID=42`
- set a flag `--writer-id 42`
- use the API (not convered here)
- use the CLI
```shell
influxdb_iox writer set 42
```

To write data, you need to create a database. You can do so via the API or using the CLI. For example, to create a database called `company_sensors` with a 100MB mutable buffer, use this command:

```shell
influxdb_iox database create company_sensors
```

Data can be stored in InfluxDB IOx by sending it in [line protocol]
format to the `/api/v2/write` endpoint or using the CLI. For example,
here is a command that will send the data in the
`tests/fixtures/lineproto/metrics.lp` file in this repository,
assuming that you're running the server on the default port into
the `company_sensors` database, you can use:

```shell
influxdb_iox database write company_sensors tests/fixtures/lineproto/metrics.lp
```

To query data stored in the `company_sensors` database:

```shell
influxdb_iox database query company_sensors "SELECT * FROM cpu LIMIT 10"
```

### Using the CLI

To ease deloyment, IOx is packaged as a combined binary which has
commands to start the IOx server as well as a CLI interface for
interacting with and configuring such servers.

The CLI itself is documented via extensive built in help which you can
access by runing `influxdb_iox --help`


### InfluxDB 2.0 compatibility

InfluxDB IOx allows seamless interoperability with InfluxDB 2.0.

InfluxDB 2.0 stores data in organization and buckets, but InfluxDB IOx
stores data in named databases. IOx maps `organization` and `bucket`
to a database named with the two parts separated by an underscore
(`_`): `organization_bucket`.

Here's an example using [`curl`] command to send the same data into
the `company_sensors` database using the InfluxDB 2.0 `/api/v2/write`
API:

```shell
curl -v "http://127.0.0.1:8080/api/v2/write?org=company&bucket=sensors" --data-binary @tests/fixtures/lineproto/metrics.lp
```

[line protocol]: https://docs.influxdata.com/influxdb/v2.0/reference/syntax/line-protocol/
[`curl`]: https://curl.se/


### Health Checks

The HTTP API exposes a healthcheck endpoint at `/health`

```console
$ curl http://127.0.0.1:8080/health
OK
```

The gRPC API implements the [gRPC Health Checking Protocol](https://github.com/grpc/grpc/blob/master/doc/health-checking.md). This can be tested with [grpc-health-probe](https://github.com/grpc-ecosystem/grpc-health-probe)

```console
$ grpc_health_probe -addr 127.0.0.1:8082 -service influxdata.platform.storage.Storage
status: SERVING
```

### Manually calling gRPC API

If you want to manually invoke one of the gRPC APIs, you can use any gRPC CLI client;
a good one is [grpcurl](https://github.com/fullstorydev/grpcurl).

Tonic (the gRPC server library we're using) currently doesn't have support for gRPC reflection,
hence you must pass all `.proto` files to your client. You can find a conventient `grpcurl` wrapper
that does that in the `scripts` directory:

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
