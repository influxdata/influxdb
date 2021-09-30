# Logging in IOx

## Basic Usage

IOx can produce logs at various different levels of verbosity, this can be controlled in one of two ways:

* The `-v` option can be specified one or more times to increase the log verbosity through a set of pre-defined filters
* A custom filter can be specified with `--log-filter` or the `LOG_FILTER` environment variable

The full format filter format is
described [here](https://tracing.rs/tracing_subscriber/filter/struct.envfilter#directives)

Some examples

```bash
# Default verbosity
$ ./influxdb_iox run
# More verbose
$ ./influxdb_iox run -v
# Even more verbose
$ ./influxdb_iox run -vv
# Everything!!
$ ./influxdb_iox run --log-filter trace
# Default info, but debug within http module
$ ./influxdb_iox run --log-filter info,influxdb_iox::influxdb_ioxd::http=debug
```

Additionally, the output format can be controlled with `--log-format`

```bash
$ ./influxdb_iox run --log-filter debug --log-format logfmt
```

## Developer Guide

IOx makes use of Rust's [tracing](https://docs.rs/tracing) ecosystem to output application logs to stdout. It
additionally makes use of [tracing-log](https://docs.rs/tracing-log) to ensure that crates writing events to
the [log](docs.rs/log/) facade are also rendered to stdout.

### Macros

As described in the [tracing documentation](https://docs.rs/tracing/0.1.28/tracing/#using-the-macros) macros are
provided to log events, e.g. `info!`, `error!`. Additionally, spans can be created that add context to all events
contained within them<sup>1.</sup>.

```rust
#[tracing::instrument]
fn say_hello(name: &str) {
    // This is good - fields come first!
    info!(name, "hello there");

    // This is alright, but less structured / useful
    info!("hello there {}", name);
}
```

### Filtering

IOx makes use of [tracing-subscriber::EnvFilter](https://tracing.rs/tracing_subscriber/filter/struct.envfilter) to
selectively enable or disable log callsites at runtime. In most cases if a callsite is not enabled by the filter, it
will not perform any string formatting and has minimal runtime overhead<sup>2.</sup>.

This is exposed as the `LOG_FILTER` environment variable, or the `--log-filter` command line argument. The format is
described [here](https://tracing.rs/tracing_subscriber/filter/struct.envfilter#directives).

Additionally [compile time features](https://tracing.rs/tracing/level_filters/index.html#compile-time-filters) are used
to strip out certain log levels from the final binary. At the time of writing `release_max_level_debug` is in use, which
will strip out all trace level callsites from the release binary.

### Format

IOx supports logging in many formats. For a list run `influxdb_iox run --help` and view the help output
for `--log-format`.

<sup>1.</sup> This span propagation uses thread-local storage and therefore does not automatically carry across
thread-boundaries (e.g. `tokio::spawn`)

<sup>2.</sup> Filters dependent on an event's attributes, called dynamic filters, will still perform attribute
formatting, even if the log is later discarded, unless a non-dynamic filter allows skipping the callsite entirely
