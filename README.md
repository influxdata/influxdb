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

We're also hosting monthly tech talks and community office hours on the project on the 2nd Wednesday of the month at 8:30 AM Pacific Time. The first [InfluxDB IOx Tech Talk is on December 9th and you can find details here](https://www.influxdata.com/community-showcase/influxdb-tech-talks/).

## Quick Start

To compile and run InfluxDB IOx from source, you'll need a Rust compiler and a `flatc` FlatBuffers
compiler.

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

By default, `rustup` will install the latest stable verison of Rust. InfluxDB IOx is currently
using a nightly version of Rust to get performance benefits from the unstable `simd` feature. The
exact nightly version is specified in the `rust-toolchain` file. When you're in the directory
containing this repository's code, `rustup` will look in the `rust-toolchain` file and
automatically install and use the correct Rust version for you. Test this out with:

```shell
rustc --version
```

and you should see a nightly version of Rust!

### Installing `flatc`

InfluxDB IOx uses the [FlatBuffer] serialization format for its write-ahead log. The [`flatc`
compiler] reads the schema in `generated_types/wal.fbs` and generates the corresponding Rust code.

Install `flatc` >= 1.12.0 with one of these methods as appropriate to your operating system:

* Using a [Windows binary release]
* Using the [`flatbuffers` package for conda]
* Using the [`flatbuffers` package for Arch Linux]
* Using the [`flatbuffers` package for Homebrew]

Once you have installed the packages, you should be able to run:

```shell
flatc --version
```

and see the version displayed.

You won't have to run `flatc` directly; once it's available, Rust's Cargo build tool manages the
compilation process by calling `flatc` for you.

[FlatBuffer]: https://google.github.io/flatbuffers/
[`flatc` compiler]: https://google.github.io/flatbuffers/flatbuffers_guide_using_schema_compiler.html
[Windows binary release]: https://github.com/google/flatbuffers/releases
[`flatbuffers` package for conda]: https://anaconda.org/conda-forge/flatbuffers
[`flatbuffers` package for Arch Linux]: https://www.archlinux.org/packages/community/x86_64/flatbuffers/
[`flatbuffers` package for Homebrew]: https://github.com/Homebrew/homebrew-core/blob/HEAD/Formula/flatbuffers.rb

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

**OPTIONAL:** There are a number of configuration variables you can choose to customize by
specifying values for environment variables in a `.env` file. To get an example file to start from,
run:

```shell
cp docs/env.example .env
```

then edit the newly-created `.env` file.

For development purposes, the most relevant environment variables are the `INFLUXDB_IOX_DB_DIR` and
`TEST_INFLUXDB_IOX_DB_DIR` variables that configure where files are stored on disk. The default
values are shown in the comments in the example file; to change them, uncomment the relevant lines
and change the values to the directories in which you'd like to store the files instead:

```shell
INFLUXDB_IOX_DB_DIR=/some/place/else
TEST_INFLUXDB_IOX_DB_DIR=/another/place
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
cargo run
```

When compiling for performance testing, build in release mode by using:

```shell
cargo build --release
```

which will create the corresponding binary in `target/release`:

```shell
./target/release/influxdb_iox
```

Similarly, you can do this in one step with:

```shell
cargo run --release
```

The server will, by default, start an HTTP API server on port `8080` and a gRPC server on port
`8082`.

### Writing and Reading Data

Data can be stored in InfluxDB IOx by sending it in [line protocol] format to the `/api/v2/write`
endpoint. Data is stored by organization and bucket names. Here's an example using [`curl`] with
the organization name `company` and the bucket name `sensors` that will send the data in the
`tests/fixtures/lineproto/metrics.lp` file in this repository, assuming that you're running the
server on the default port:

```shell
curl -v "http://127.0.0.1:8080/api/v2/write?org=company&bucket=sensors" --data-binary @tests/fixtures/lineproto/metrics.lp
```

[line protocol]: https://docs.influxdata.com/influxdb/v2.0/reference/syntax/line-protocol/
[`curl`]: https://curl.se/

To query stored data, use the `/api/v2/read` endpoint with a SQL query. This example will return
all data in the `company` organization's `sensors` bucket for the `processes` measurement:

```shell
curl -v -G -d 'org=company' -d 'bucket=sensors' --data-urlencode 'sql_query=select * from processes' "http://127.0.0.1:8080/api/v2/read"
```

## Contributing

We welcome community contributions from anyone!

Read our [Contributing Guide](CONTRIBUTING.md) for instructions on how to make your first contribution.
