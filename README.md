<div align="center">
 <picture>
    <source media="(prefers-color-scheme: light)" srcset="assets/influxdb-logo.png">
    <source media="(prefers-color-scheme: dark)" srcset="assets/influxdb-logo-dark.png">
    <img src="assets/influxdb-logo.png" alt="InfluxDB Logo" width="600">
  </picture>
 <p>
InfluxDB is the leading open source time series database for metrics, events, and real-time analytics.</p>

</div>

## Learn InfluxDB
[Documentation](https://docs.influxdata.com/) | [Community Forum](https://community.influxdata.com/) | [Community Slack](https://www.influxdata.com/slack/) | [Blog](https://www.influxdata.com/blog/) | [InfluxDB University](https://university.influxdata.com/) | [YouTube](https://www.youtube.com/@influxdata8893)

Try **InfluxDB Cloud** for free and get started fast with no local setup required. Click [here](https://cloud2.influxdata.com/signup) to start building your application on InfluxDB Cloud.

## Install

We have nightly and versioned Docker images, Debian packages, RPM packages, and tarballs of InfluxDB available at the [InfluxData downloads page](https://portal.influxdata.com/downloads/). We also provide the `influx` command line interface (CLI) client as a separate binary available at the same location.

If you are interested in building from source, see the [building from source](CONTRIBUTING.md#building-from-source) guide for contributors.

## Get Started
For a complete getting started guide, please see our full [online documentation site](https://docs.influxdata.com/influxdb/latest/).


To write and query data or use the API in any way, you'll need to first create a user, credentials, organization, and bucket. Everything in InfluxDB is organized under a concept of an organization, as the API is designed to be multi-tenant. Buckets represent where you store time series data — they are synonymous with what was previously in InfluxDB 1.x’s database and retention policy.


The simplest way to get set up is to point your browser to http://localhost:8086 and go through the prompts.

## Flux
Flux is an open source functional data scripting language designed for querying, analyzing, and acting on data. Flux is supported in InfluxDB 1.x and 2.x, but is not supported in v3. For users who are interested in transitioning to InfluxDB 3.0 and want to future-proof their code, we suggest using InfluxQL. 


The source for Flux is [available on GitHub](https://github.com/influxdata/flux). 

## Additional Resources
- [InfluxDB Tips and Tutorials](https://www.influxdata.com/blog/category/tech/influxdb/)
- [InfluxDB Essentials Course](https://university.influxdata.com/courses/influxdb-essentials-tutorial/)
- [Exploring InfluxDB Cloud Course](https://university.influxdata.com/courses/exploring-influxdb-cloud-tutorial/)