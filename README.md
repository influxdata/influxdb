<div align="center">
 <picture>
    <source media="(prefers-color-scheme: light)" srcset="assets/influxdb-logo.png">
    <source media="(prefers-color-scheme: dark)" srcset="assets/influxdb-logo-dark.png">
    <img src="assets/influxdb-logo.png" alt="InfluxDB Logo" width="600">
  </picture>
 <p>InfluxDB OSS v2 — open source time series platform powered by the TSM storage engine.</p>

</div>

> [!NOTE]
> This branch documents an earlier version of InfluxDB OSS. [InfluxDB 3 Core](https://github.com/influxdata/influxdb/tree/main) is the latest stable version. InfluxDB 3 Core includes [compatibility APIs for v2 and v1 write workloads](https://docs.influxdata.com/influxdb3/core/reference/api/).

**Version:** 2.x\
**Branch:** `main-2.x`\
**Storage engine:** TSM (Time-Structured Merge)\
**Query languages:** Flux, InfluxQL\
**API:** [v2 REST API](https://docs.influxdata.com/influxdb/v2/reference/api/) (write, query, manage)\
**Compatible with:** InfluxDB 1.x write API and query (InfluxQL) API\
**License:** [MIT](LICENSE)\
**Documentation:** [docs.influxdata.com/influxdb/v2/](https://docs.influxdata.com/influxdb/v2/)

## Learn InfluxDB
[Documentation](https://docs.influxdata.com/) | [Community Forum](https://community.influxdata.com/) | [Community Slack](https://www.influxdata.com/slack/) | [Blog](https://www.influxdata.com/blog/) | [InfluxDB University](https://university.influxdata.com/) | [YouTube](https://www.youtube.com/@influxdata8893)

## Install

For Docker images, Debian packages, RPM packages, and tarballs, see the [InfluxData downloads page](https://portal.influxdata.com/downloads/). The `influx` command line interface (CLI) client is available as a separate binary at the same location.

If you are interested in building from source, see the [building from source](CONTRIBUTING.md#building-from-source) guide for contributors.

## Get Started
For a complete getting started guide, see the [InfluxDB v2 documentation](https://docs.influxdata.com/influxdb/v2/get-started/).


In InfluxDB v2, data and resources belong to an _organization_. You store time series data in _buckets_ (equivalent to a database and retention policy in InfluxDB 1.x). To get started, create a user, organization, and bucket — either through the UI at `http://localhost:8086` or with the `influx setup` CLI command.

## Flux
[Flux](https://github.com/influxdata/flux) is an open source functional data scripting language designed for querying, analyzing, and acting on data. Flux is supported in InfluxDB 1.x and 2.x, but is not supported in InfluxDB 3. If you plan to migrate to InfluxDB 3 in the future, we recommend using [InfluxQL](https://docs.influxdata.com/influxdb/v2/query-data/influxql/) instead of Flux with InfluxDB v2.
