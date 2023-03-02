# `iox_data_generator`

The `iox_data_generator` tool creates random data points according to a specification and loads them
into an `iox` instance to simulate real data.

To build and run, [first install Rust](https://www.rust-lang.org/tools/install). Then from root of the `influxdb_iox` repo run:

```
cargo build --release
```

And the built binary has command line help:

```
./target/release/iox_data_generator --help
```

For examples of specifications see the [schemas folder](schemas). The [full_example](schemas/full_example.toml) is the
most comprehensive with comments and example output.
