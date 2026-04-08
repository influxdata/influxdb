# InfluxDB 3 Core

<div align="center">
 <picture>
    <source media="(prefers-color-scheme: light)" srcset="assets/influxdb-logo.png">
    <source media="(prefers-color-scheme: dark)" srcset="assets/influxdb-logo-dark.png">
    <img src="assets/influxdb-logo.png" alt="InfluxDB 3 Core" width="600">
  </picture>
 <p>Open source time series database for real-time events, analytics, and monitoring — powered by Apache Arrow, DataFusion, and Parquet.</p>

  [![License: MIT/Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue)](LICENSE)
  [![Discord](https://img.shields.io/badge/Discord-join_chat-5865F2?logo=discord&logoColor=white)](https://discord.gg/vZe2w2Ds8B)
</div>

InfluxDB 3 Core is a database built to collect, process, transform, and store event and time series data. It is ideal for use cases that require real-time ingest and fast query response times to build user interfaces, monitoring, and automation solutions.

Common use cases include:

- Monitoring sensor data
- Server monitoring
- Application performance monitoring
- Network monitoring
- Financial market and trading analytics
- Behavioral analytics

InfluxDB is optimized for scenarios where near real-time data monitoring is essential and queries 
need to return quickly to support user experiences such as dashboards and interactive user interfaces.

InfluxDB 3 Core’s feature highlights include:

- Diskless architecture with object storage support (or local disk with no dependencies)
- Fast query response times (under 10ms for last-value queries, or 30ms for distinct metadata)
- Embedded Python VM for plugins and triggers
- Parquet file persistence
- Compatibility with InfluxDB 1.x and 2.x write APIs
- Compatibility with InfluxDB 1.x query API (InfluxQL)
- SQL query engine with support for FlightSQL and HTTP query API

**Storage format:** Apache Parquet on object storage (S3, Azure, GCP) or local disk\
**Query languages:** SQL, InfluxQL, Flight SQL\
**Write format:** Line protocol\
**API:** HTTP on port 8181\
**Built with:** Rust, Apache Arrow, DataFusion\
**Compatible with:** InfluxDB 1.x and 2.x write APIs, InfluxDB 1.x query API (InfluxQL)

## Other InfluxDB Versions

This repository contains multiple InfluxDB versions on separate branches:

| Version | Branch | Query Languages | Documentation |
|---------|--------|-----------------|---------------|
| **v3 Core (this branch)** | [`main`](https://github.com/influxdata/influxdb/tree/main) | SQL, InfluxQL | [docs.influxdata.com/influxdb3/core/](https://docs.influxdata.com/influxdb3/core/) |
| v2.x | [`main-2.x`](https://github.com/influxdata/influxdb/tree/main-2.x) | Flux, InfluxQL | [docs.influxdata.com/influxdb/v2/](https://docs.influxdata.com/influxdb/v2/) |
| v1.x | [`master-1.x`](https://github.com/influxdata/influxdb/tree/master-1.x) | InfluxQL, Flux | [docs.influxdata.com/influxdb/v1/](https://docs.influxdata.com/influxdb/v1/) |

## Project Status

InfluxDB 3 Core has been [generally available since April 2025](https://www.influxdata.com/blog/influxdb-3-oss-ga/). See [`v3.*` tags](https://github.com/influxdata/influxdb/tags) and [release notes](https://docs.influxdata.com/influxdb3/core/release-notes/) for the latest version.

Join the [InfluxDB3 Discord](https://discord.gg/vZe2w2Ds8B) or the public channels below to share your feedback, feature requests, and bug reports.

## Installation
Docker images, Debian packages, RPM packages, and tarballs are available on the [InfluxData downloads page](https://portal.influxdata.com/downloads/).

- For InfluxDB 3 Core, see the [getting started guide](https://docs.influxdata.com/influxdb3/core/get-started/).
- For InfluxDB 3 Enterprise, see the [getting started guide](https://docs.influxdata.com/influxdb3/enterprise/get-started/).
- For v2.x, see the [`main-2.x` branch](https://github.com/influxdata/influxdb/tree/main-2.x).
- For v1.x, see the [`master-1.x` branch](https://github.com/influxdata/influxdb/tree/master-1.x).

If you are interested in building from source, see the [building from source](CONTRIBUTING.md#building-from-source) guide for contributors.


## Support

For community support and feedback channels, see [Bug reports and feedback](https://docs.influxdata.com/influxdb3/core/#bug-reports-and-feedback).

## License
The open source software we build is licensed under the permissive MIT or Apache 2 licenses at the user’s choosing. We’ve long held the view that our open source code should be truly open and our commercial code should be separate and closed.


## Interested in joining the team building InfluxDB?
Check out current job openings at [www.influxdata.com/careers](https://www.influxdata.com/careers) today!
