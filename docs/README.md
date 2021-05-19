# InfluxDB IOx Documentation

This directory contains internal design documentation of potential
interest for those who wish to understand how the code works. It is
not intended to be general user facing documentation

## IOx Tech Talks

We hold monthly Tech Talks that explain the project's technical underpinnings. You can register for the [InfluxDB IOx Tech Talks here](https://www.influxdata.com/community-showcase/influxdb-tech-talks/), or you can find links to previous sessions below:

* December 2020: Rusty Introduction to Apache Arrow [recording](https://www.youtube.com/watch?v=dQFjKa9vKhM)
* Jan 2021: Data Lifecycle in InfluxDB IOx & How it Uses Object Storage for Persistence [recording](https://www.youtube.com/watch?v=KwdPifHC1Gc)
* February 2021: Intro to the InfluxDB IOx Read Buffer [recording](https://www.youtube.com/watch?v=KslD31VNqPU) [slides](https://www.slideshare.net/influxdata/influxdb-iox-tech-talks-intro-to-the-influxdb-iox-read-buffer-a-readoptimized-inmemory-query-execution-engine)
* March 2021: Query Engine Design and the Rust-Based DataFusion in Apache Arrow [recording](https://www.youtube.com/watch?v=K6eCAVEk4kU) [slides](https://www.slideshare.net/influxdata/influxdb-iox-tech-talks-query-engine-design-and-the-rustbased-datafusion-in-apache-arrow-244161934)

## Table of Contents:

* Rust style and Idiom guide: [style_guide.md](style_guide.md)
* Tracing and logging Guide: [tracing.md](tracing.md)
* Profiling Guide: [profiling.md](profiling.md)
* How InfluxDB IOx manages the lifecycle of time series data: [data_management.md](data_management.md)
* Thoughts on parquet encoding and compression for timeseries data: [encoding_thoughts.md](encoding_thoughts.md)
* Thoughts on using multiple cores: [multi_core_tasks.md](multi_core_tasks.md)
* [Query Engine Docs](../query/README.md)
* [Testing documentation](testing.md) for developers of IOx
* [Regenerating Flatbuffers code](regenerating_flatbuffers.md) when updating the version of the `flatbuffers` crate
* Protobuf tips and tricks: [Protobuf](protobuf.md).
* Catalog Persistence: [`catalog_persistence.md`](catalog_persistence.md).
* SQL command line tips and tricks: [SQL](sql.md).
