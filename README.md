<div align="center">
 <picture>
    <source media="(prefers-color-scheme: light)" srcset="assets/influxdb-logo.png">
    <source media="(prefers-color-scheme: dark)" srcset="assets/influxdb-logo-dark.png">
    <img src="assets/influxdb-logo.png" alt="InfluxDB Logo" width="600">
  </picture>
 <p>
</div>

InfluxDB Core is a database built to collect, process, transform, and store event and time series data. It is ideal for use cases that require real-time ingest and fast query response times to build user interfaces, monitoring, and automation solutions.

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

## Project Status

InfluxDB 3 Core is in public alpha and available for testing and feedback, but is not meant for 
production use. During the alpha period we may make breaking changes that will require you to blow 
away your data and start over. You should have copies of your data in other places during the alpha 
period. Both the product and this documentation are works in progress. New builds get created on 
every merge into main, so things will be moving quickly for the next month or so. We welcome and 
encourage your input about your experience with the alpha. Join the [InfluxDB3 Discord](https://discord.gg/vZe2w2Ds8B) 
or the public channels below.

See the [InfluxDB 3 alpha release announcement here](https://www.influxdata.com/blog/influxdb3-open-source-public-alpha/) 
or dig into the [InfluxDB 3 getting started guide here](https://docs.influxdata.com/influxdb3/core/get-started/).

## Learn InfluxDB
[Documentation](https://docs.influxdata.com/) | [Community Forum](https://community.influxdata.com/) | [Community Slack](https://www.influxdata.com/slack/) | [Blog](https://www.influxdata.com/blog/) | [InfluxDB University](https://university.influxdata.com/) | [YouTube](https://www.youtube.com/@influxdata8893)

Try **InfluxDB Cloud** for free and get started fast with no local setup required. Click [here](https://cloud2.influxdata.com/signup) to start building your application on InfluxDB Cloud.


## Installation
We have nightly and versioned Docker images, Debian packages, RPM packages, and tarballs of InfluxDB available on the [InfluxData downloads page](https://portal.influxdata.com/downloads/). We also provide the InfluxDB command line interface (CLI) client as a separate binary available at the same location.

- For v1 installation, use the [main 1.x branch](https://github.com/influxdata/influxdb/tree/master-1.x) or [install InfluxDB OSS directly](https://docs.influxdata.com/influxdb/v1/introduction/install/#installing-influxdb-oss).
- For v2 installation, use the [main 2.x branch](https://github.com/influxdata/influxdb/tree/main-2.x).
- For InfluxDB 3 Core alpha see the [InfluxDB 3 Core getting started guide](https://docs.influxdata.com/influxdb3/core/get-started/).

If you are interested in building from source, see the [building from source](https://github.com/influxdata/influxdb/blob/main-2.x/CONTRIBUTING.md#building-from-source) guide for contributors.

To begin using InfluxDB, visit our [Getting Started with InfluxDB](https://docs.influxdata.com/influxdb/v1/introduction/get-started/) documentation.


## License
The open source software we build is licensed under the permissive MIT or Apache 2 licenses at the user's choosing. We’ve long held the view that our open source code should be truly open and our commercial code should be separate and closed. 


## Interested in joining the team building InfluxDB?
Check out current job openings at [www.influxdata.com/careers](https://www.influxdata.com/careers) today!