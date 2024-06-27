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

## An Open-Source Time Series Database

InfluxDB is an open source **time series database** with
**no external dependencies**. It's useful for recording metrics,
events, and performing analytics.

This branch, `master-1.x`, is for InfluxDB 1.x. If you'd like to leverage InfluxDB 2.x, please switch to the `main-2.x` branch. The `main` branch is currently being used for active development on the open source version of InfluxDB 3.x and is **not** ready for production use. 

## Features

* Built-in [HTTP API](https://docs.influxdata.com/influxdb/latest/guides/writing_data/) so you don't have to write any server side code to get up and running.
* Data can be tagged, allowing very flexible querying.
* SQL-like query language.
* Simple to install and manage, and fast to get data in and out.
* It aims to answer queries in real-time. That means every data point is
  indexed as it comes in and is immediately available in queries that
  should return in < 100ms.

## Installation

We recommend installing InfluxDB using one of the [pre-built packages](https://influxdata.com/downloads/#influxdb). Then start InfluxDB using:

* `service influxdb start` if you have installed InfluxDB using an official Debian or RPM package.
* `systemctl start influxdb` if you have installed InfluxDB using an official Debian or RPM package, and are running a distro with `systemd`. For example, Ubuntu 15 or later.
* `$GOPATH/bin/influxd` if you have built InfluxDB from source.

## Getting Started

### Create your first database

```
curl -XPOST "http://localhost:8086/query" --data-urlencode "q=CREATE DATABASE mydb"
```

### Insert some data
```
curl -XPOST "http://localhost:8086/write?db=mydb" \
-d 'cpu,host=server01,region=uswest load=42 1434055562000000000'

curl -XPOST "http://localhost:8086/write?db=mydb" \
-d 'cpu,host=server02,region=uswest load=78 1434055562000000000'

curl -XPOST "http://localhost:8086/write?db=mydb" \
-d 'cpu,host=server03,region=useast load=15.4 1434055562000000000'
```

### Query for the data
```JSON
curl -G "http://localhost:8086/query?pretty=true" --data-urlencode "db=mydb" \
--data-urlencode "q=SELECT * FROM cpu WHERE host='server01' AND time < now() - 1d"
```

### Analyze the data
```JSON
curl -G "http://localhost:8086/query?pretty=true" --data-urlencode "db=mydb" \
--data-urlencode "q=SELECT mean(load) FROM cpu WHERE region='uswest'"
```

## Documentation

* Read more about the [design goals and motivations of the project](https://docs.influxdata.com/influxdb/latest/).
* Follow the [getting started guide](https://docs.influxdata.com/influxdb/latest/introduction/getting_started/) to learn the basics in just a few minutes.
* Learn more about [InfluxDB's key concepts](https://docs.influxdata.com/influxdb/latest/concepts/key_concepts/).

## Contributing

If you're feeling adventurous and want to contribute to InfluxDB, see our [contributing doc](https://github.com/influxdata/influxdb/blob/master-1.x/CONTRIBUTING.md) for info on how to make feature requests, build from source, and run tests.

## Licensing

See [LICENSE](./LICENSE) and [DEPENDENCIES](./DEPENDENCIES).

## Looking for Support?

InfluxDB offers a number of services to help your project succeed. We offer Developer Support for organizations in active development, Managed Hosting to make it easy to move into production, and Enterprise Support for companies requiring the best response times, SLAs, and technical fixes. Visit our [support page](https://influxdata.com/services/) or contact [sales@influxdb.com](mailto:sales@influxdb.com) to learn how we can best help you succeed.
