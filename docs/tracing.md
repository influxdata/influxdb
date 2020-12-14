# Tracing in IOx

IOx makes use of Rust's tracing ecosystem to output both application logs and
tracing events to Jaeger for distributed request correlation.

## The Components

The `tracing` framework aims to provide individual, pluggable crates that are
composed together to build a feature set custom to the application's needs.

### The `tracing` framework

Rust has the excellent [`tracing` crate] to facilitate collection of application
events with associated contextual information. 

The most user visible components provided are the `debug!()`, `info!()`, etc
function-like macros, and the `#[tracing::instrument]` attribute macro.

The `info!()` macro and friends emit an "event" - a point in time event tagged
with relevant data.

If you add the `instrument` macro to a function, the tracing framework records
the function entry as a "span" that covers the duration of the call, and records
the timestamp when it returns. Any events or sub-spans emitted further down the
call stack inside this function's span are recorded as children of this span to
track causality.

The `tracing` crate focuses on providing a good API for emitting events & spans.
Consumers of the tracing data are called `Subscribers` and they are implemented
in other crates - there are subscribers that consume events and emit logs, some
that use the function entry spans for timing/metric data, and more every day!

### OpenTelemetry

[OpenTelemetry] is a relatively new standard to encourage interoperability within
the telemetry/tracing ecosystem. The `tracing-opentelemetry` crate subscribes to
the data produced by the `tracing` crate.

It's essentially an adaptor, consuming from `tracing` and converting them to
OpenTelemetry compatible events.

There are many [configuration options] supported at runtime.

### Jaeger (`opentelemetry_jaeger`)

[Jaeger] is a widely used telemetry collector with its own wire protocol,
separate from OpenTelemetry. While they're working on adding support for the
OpenTelemetry standard, it is not yet fully integrated (though I believe there
are builds available.)

The [`opentelemetry_jaeger` crate] sits at the end of the chain - the `tracing`
events are converted to `tracing-opentelemetry` events, which dispatches them to
this crate to be emitted to a listening Jaeger service.

## Using it

### Running IOx

First off, when running IOx you need to choose how much information you want.
You do this by setting the `RUST_LOG="debug"` environment variable (or `info`,
`error`, etc) or passing `-v` or `-vv` on the command line. The `RUST_LOG`
environment variable provides [granular control] over what log verbosity is
configured for individual internal components (tip: try `RUST_LOG="iox=debug"`!)

You can also choose to collect traces in Jaeger - this helps visualise
request-scoped events and provides timing information, and is generally a pretty
helpful tool when debugging. To enable Jaeger tracing output, set the following
environment variables:

```shell
OTEL_SERVICE_NAME="iox" # Defaults to iox if not specified

# No default, must be set
OTEL_EXPORTER_JAEGER_AGENT_HOST="jaeger.influxdata.net"
OTEL_EXPORTER_JAEGER_AGENT_PORT="6831"
```

### Working on IOx

When you're writing code, you should liberally use `debug` level tracing, as
well as `info` and above for information that is important to the user. This
helps greatly when tracking down any problems or to simply understand where
requests wind up in the codebase - this is particularly important when requests
span multiple servers!

Instrument your important functions at `info` - these provide a logical request
call stack rather than a _function_ call stack, so you can construct meaningful
traces without lots of noise.

The `trace!()` level is useful to instrument areas of code for development or
testing that should not wind up in the release binary - as such, the `trace`
level is compiled at when building a "release" binary. As a guideline:

* `trace!()` for debug builds and verbose messaging, used liberally as required.
  Compiled out in release builds.
* `debug!()` for events that would help tracking down bugs, but are not useful
  to the user
* `info!()` & above for info useful to the user

Emit logs as you normally would, but [include contextual information] in them as
**fields** rather than interpolated strings:

```rust
#[tracing::instrument]
fn say_hello(name: &str) {
	// This is good - fields come first!
    info!(name, "hello there");

	// This is alright, but less structured / useful
    info!("hello there {}", name);
}
```

Instrumented functions wind up as distinct spans in the Jaeger trace, and all
the events emitted within them are output to stdout as log lines (with the
contextual information) as well as in the Jaeger span.

Instrumented functions also record the call arguments automatically (though you
can [control it]) with args printed using `Display` at `level=info` and above,
or `Debug` being used for `level=debug`. For example:

If your function returns a `Result<T, E>` and `E: Display` then you can record
the error too:

```rust
#[tracing::instrument(err)]
fn my_function(arg: usize) -> Result<(), std::io::Error> {
    Ok(())
}
```

You can also instrument a function only at the "debug" level (`info` is the
default):

```rust
#[tracing::instrument(level = "debug")]
fn say_hello() {}
```

## Good to Know

* If you have an outer func that is instrumented with `level=debug`, and an
  inner that is instrumented for `level=info`, the inner shows up when
  `level=info` even if the outer does not (children are independent of their
  parents).

* You can control the log level of the Jaeger events with the OpenTelemetry
  config envs, but ultimately the `RUST_LOG` env sets the log level filter - you
  cannot configure telemetry to emit at a log level lower than `RUST_LOG`.

* Be careful passing around sensitive arguments - instrumented functions will
  record them! It's best to wrap them in some wrapper type that implements
  neither `Display` nor `Debug` - then they can never be printed anywhere,
  including in tracing!

[configuration options]: https://github.com/open-telemetry/opentelemetry-specification/blob/master/specification/sdk-environment-variables.md#opentelemetry-environment-variable-specification
[control it]: https://docs.rs/tracing/0.1.22/tracing/attr.instrument.html
[granular control]: https://docs.rs/tracing-subscriber/0.2.15/tracing_subscriber/filter/struct.EnvFilter.html
[`tracing` crate]: https://docs.rs/tracing/0.1.22/tracing/
[OpenTelemetry]: https://opentelemetry.io/
[Jaeger]: https://www.jaegertracing.io/
[`opentelemetry_jaeger` crate]: https://docs.rs/opentelemetry/0.10.0/opentelemetry/
[include contextual information]: https://docs.rs/tracing/0.1.22/tracing/#recording-fields
