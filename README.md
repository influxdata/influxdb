# Chronograf

Chronograf is an open-source web application written in Go and React.js that provides the tools to visualize your monitoring data and easily create alerting and automation rules.

![Chronograf](https://github.com/influxdata/chronograf/blob/master/docs/images/overview-readme.png)

## Features

### Host List

* List and sort hosts
* View general CPU and load stats
* View and access dashboard templates for configured apps

### Dashboard Templates

Chronograf's [pre-canned dashboards](https://github.com/influxdata/chronograf/tree/master/canned) for the supported [Telegraf](https://github.com/influxdata/telegraf) input plugins.
Currently, Chronograf offers dashboard templates for the following Telegraf input plugins:

* Apache
* Consul
* Docker
* Elastic
* etcd
* HAProxy
* IIS
* InfluxDB
* Kubernetes
* Memcached
* MongoDB
* MySQL
* Network
* NGINX
* NSQ
* Ping
* PostgreSQL
* Processes
* RabbitMQ
* Redis
* Riak
* [System](https://github.com/influxdata/telegraf/blob/master/plugins/inputs/system/SYSTEM_README.md)
    * [CPU](https://github.com/influxdata/telegraf/blob/master/plugins/inputs/system/CPU_README.md)
    * [Disk](https://github.com/influxdata/telegraf/blob/master/plugins/inputs/system/DISK_README.md)
    * [DiskIO](https://github.com/influxdata/telegraf/blob/master/plugins/inputs/system/disk.go#L136)
    * [Memory](https://github.com/influxdata/telegraf/blob/master/plugins/inputs/system/MEM_README.md)
    * [Net](https://github.com/influxdata/telegraf/blob/master/plugins/inputs/system/net.go)
    * [Netstat](https://github.com/influxdata/telegraf/blob/master/plugins/inputs/system/NETSTAT_README.md)
    * [Processes](https://github.com/influxdata/telegraf/blob/master/plugins/inputs/system/PROCESSES_README.md)
    * [Procstat](https://github.com/influxdata/telegraf/blob/master/plugins/inputs/procstat/README.md)
* Varnish
* Windows Performance Counters

> Note: If a `telegraf` instance isn't running the `system` and `cpu` plugins the canned dashboards from that instance won't be generated.

### Data Explorer

Chronograf's graphing tool that allows you to dig in and create personalized visualizations of your data.

* Generate [InfluxQL](https://docs.influxdata.com/influxdb/latest/query_language/) statements with the query builder
* Generate and edit [InfluxQL](https://docs.influxdata.com/influxdb/latest/query_language/) statements with the raw query editor
* Create visualizations and view query results in tabular format
* Manage visualizations with exploration sessions

### Kapacitor UI

A UI for [Kapacitor](https://github.com/influxdata/kapacitor) alert creation and alert tracking.

* Simply generate threshold, relative, and deadman alerts
* Preview data and alert boundaries while creating an alert
* Configure alert destinations - Currently, Chronograf supports sending alerts to:
  * HipChat
  * OpsGenie
  * PagerDuty
  * Sensu
  * Slack
  * SMTP
  * Talk
  * Telegram
  * VictorOps
* View all active alerts at a glance on the alerting dashboard

### GitHub OAuth Login
See [Chronograf with OAuth 2.0](https://github.com/influxdata/chronograf/blob/master/docs/auth.md) for more information.

## Versions

Chronograf v1.2.0-beta1 is a beta release.
We will be iterating quickly based on user feedback and recommend using the [nightly builds](https://www.influxdata.com/downloads/) for the time being.

Spotted a bug or have a feature request?
Please open [an issue](https://github.com/influxdata/chronograf/issues/new)!

### Known Issues

The Chronograf team has identified and is working on the following issues:

* Currently, Chronograf requires users to run Telegraf's [CPU](https://github.com/influxdata/telegraf/blob/master/plugins/inputs/system/CPU_README.md) and [system](https://github.com/influxdata/telegraf/blob/master/plugins/inputs/system/SYSTEM_README.md) plugins to ensure that all Apps appear on the [HOST LIST](https://github.com/influxdata/chronograf/blob/master/docs/GETTING_STARTED.md#host-list) page.

## Installation

Check out the [INSTALLATION](https://github.com/influxdata/chronograf/blob/master/docs/INSTALLATION.md) guide to get up and running with Chronograf with as little configuration and code as possible.

We recommend installing Chronograf using one of the [pre-built packages](https://influxdata.com/downloads/#chronograf). Then start Chronograf using:

* `service chronograf start` if you have installed Chronograf using an official Debian or RPM package.
* `systemctl start chronograf` if you have installed Chronograf using an official Debian or RPM package, and are running a distro with `systemd`. For example, Ubuntu 15 or later.
* `$GOPATH/bin/chronograf` if you have built Chronograf from source.

### With Docker
To get started right away with Docker, you can pull down our latest alpha:

```sh
docker pull quay.io/influxdb/chronograf:latest
```

### From Source

* Chronograf works with go 1.7.x, node 6.x/7.x, and npm 3.x.
* Chronograf requires [Kapacitor](https://github.com/influxdata/kapacitor) 1.1.x+ to create and store alerts.

1. [Install Go](https://golang.org/doc/install)
1. [Install Node and NPM](https://nodejs.org/en/download/)
1. [Setup your GOPATH](https://golang.org/doc/code.html#GOPATH)
1. Run `go get github.com/influxdata/chronograf`
1. Run `cd $GOPATH/src/github.com/influxdata/chronograf`
1. Run `make`
1. To install run `go install github.com/influxdata/chronograf/cmd/chronograf`

## Documentation

[INSTALLATION](https://github.com/influxdata/chronograf/blob/master/docs/INSTALLATION.md) will get you up and running with Chronograf with as little configuration and code as possible.
See the [GETTING STARTED](https://github.com/influxdata/chronograf/blob/master/docs/GETTING_STARTED.md) guide to get familiar with Chronograf's main features.

Documentation for Telegraf, InfluxDB, and Kapacitor are available at https://docs.influxdata.com/.

## Contributing

Please see the [contributing guide](CONTRIBUTING.md) for details on contributing to Chronograf.
