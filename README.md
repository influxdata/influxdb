<div align="center">
 <picture>
    <source media="(prefers-color-scheme: light)" srcset="assets/influxdb-logo.png">
    <source media="(prefers-color-scheme: dark)" srcset="assets/influxdb-logo-dark.png">
    <img src="assets/influxdb-logo.png" alt="InfluxDB Logo" width="600">
  </picture>
 <p>
InfluxDB is the leading open source time series database for metrics, events, and real-time analytics.</p>

</div>

> [!NOTE]
> This branch documents an earlier version of InfluxDB OSS. [InfluxDB 3 Core](https://github.com/influxdata/influxdb/tree/main) is the latest stable version. InfluxDB 3 Core includes [compatibility APIs for v2 and v1 write workloads](https://docs.influxdata.com/influxdb3/core/reference/api/).

## InfluxDB Versions

This repository contains multiple InfluxDB versions on separate branches:

| Version | Branch | Query Languages | Documentation |
|---------|--------|----------------|---------------|
| **v1.x (this branch)** | [`master-1.x`](https://github.com/influxdata/influxdb/tree/master-1.x) | InfluxQL, Flux | [docs.influxdata.com/influxdb/v1/](https://docs.influxdata.com/influxdb/v1/) |
| v2.x | [`main-2.x`](https://github.com/influxdata/influxdb/tree/main-2.x) | Flux, InfluxQL | [docs.influxdata.com/influxdb/v2/](https://docs.influxdata.com/influxdb/v2/) |
| v3 Core | [`main`](https://github.com/influxdata/influxdb/tree/main) | SQL, InfluxQL | [docs.influxdata.com/influxdb3/core/](https://docs.influxdata.com/influxdb3/core/) |

## Technical Summary

| | |
|---|---|
| **Storage engine** | TSM (Time-Structured Merge Tree) |
| **Query languages** | InfluxQL, Flux |
| **Write format** | [Line protocol](https://docs.influxdata.com/influxdb/v1/write_protocols/line_protocol_reference/) |
| **API** | HTTP on port 8086 |
| **Configuration** | TOML (`influxdb.conf`) |

## Features

* Built-in [HTTP API](https://docs.influxdata.com/influxdb/v1/tools/api/) — no server-side code needed to get up and running.
* Data can be tagged (tag keys, tag values, field keys), allowing flexible querying.
* SQL-like query language ([InfluxQL](https://docs.influxdata.com/influxdb/v1/query_language/)) with [Flux](https://docs.influxdata.com/flux/v0/) support.
* Simple to install and manage, fast to get data in and out ([line protocol](https://docs.influxdata.com/influxdb/v1/write_protocols/line_protocol_reference/)).
* Real-time indexing — every data point is immediately available in queries.

## Installation

We recommend installing InfluxDB using one of the [pre-built packages](https://influxdata.com/downloads/#influxdb). Then start InfluxDB using:

* `service influxdb start` if you have installed InfluxDB using an official Debian or RPM package.
* `systemctl start influxdb` if you have installed InfluxDB using an official Debian or RPM package, and are running a distro with `systemd`. For example, Ubuntu 15 or later.
* `$GOPATH/bin/influxd` if you have built InfluxDB from source.

## Getting Started

### Create your first database

```bash
curl -XPOST "http://localhost:8086/query" --data-urlencode "q=CREATE DATABASE mydb"
```

### Insert some data
```bash
curl -XPOST "http://localhost:8086/write?db=mydb" \
-d 'cpu,host=server01,region=uswest load=42 1434055562000000000'

curl -XPOST "http://localhost:8086/write?db=mydb" \
-d 'cpu,host=server02,region=uswest load=78 1434055562000000000'

curl -XPOST "http://localhost:8086/write?db=mydb" \
-d 'cpu,host=server03,region=useast load=15.4 1434055562000000000'
```

### Query for the data
```bash
curl -G "http://localhost:8086/query?pretty=true" --data-urlencode "db=mydb" \
--data-urlencode "q=SELECT * FROM cpu WHERE host='server01' AND time < now() - 1d"
```

### Analyze the data
```bash
curl -G "http://localhost:8086/query?pretty=true" --data-urlencode "db=mydb" \
--data-urlencode "q=SELECT mean(load) FROM cpu WHERE region='uswest'"
```

## Documentation

* Follow the [getting started guide](https://docs.influxdata.com/influxdb/v1/introduction/get-started/) to learn the basics in just a few minutes.
* Learn more about [InfluxDB's key concepts](https://docs.influxdata.com/influxdb/v1/concepts/key_concepts/).

## Contributing

If you're feeling adventurous and want to contribute to InfluxDB, see our [contributing doc](https://github.com/influxdata/influxdb/blob/master-1.x/CONTRIBUTING.md) for info on how to make feature requests, build from source, and run tests.

## Licensing

See [LICENSE](./LICENSE) and [DEPENDENCIES](./DEPENDENCIES).

## Support

For community support, feedback channels, and documentation requests, see the [InfluxDB v1 documentation](https://docs.influxdata.com/influxdb/v1/#bug-reports-and-feedback).
