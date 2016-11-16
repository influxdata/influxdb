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
* System
* Docker
* HAProxy
* InfluxDB
* Kubernetes
* System
* Memcached
* MongoDB
* MySQL
* Network
* NGINX
* NSQ
* Ping
* PostgreSQL
* Processes
* Redis
* Riak
* Windows Performance Counters
* IIS
* etcd
* Elastic
* Varnish

### Data Explorer

Chronograf's graphing tool that allows you to dig in and create personalized visualizations of your data.

* Generate [InfluxQL](https://docs.influxdata.com/influxdb/v1.1/query_language/) statements with the query builder
* Create visualizations and view query results in tabular format
* Manage visualizations with exploration sessions

### Kapacitor UI

A UI for [Kapacitor](https://github.com/influxdata/kapacitor) alert creation and alert tracking.

* Simply generate threshold, relative, and deadman alerts
* Preview data and alert boundaries while creating an alert
* Configure alert destinations - Currently, Chronograf supports sending alerts to:
  * HipChat
  * PagerDuty
  * Sensu
  * Slack
  * SMTP
  * Telegram
  * VictorOps
* View all active alerts at a glance on the alerting dashboard

## Versions

Chronograf v1.1.0-alpha is an [alpha release](https://www.influxdata.com/announcing-the-new-chronograf-a-ui-for-the-tick-stack-and-a-complete-open-source-monitoring-solution/).
We will be iterating quickly based on user feedback and recommend using the [nightly builds](https://www.influxdata.com/downloads/) for the time being.

Spotted a bug or have a feature request?
Please open [an issue](https://github.com/influxdata/chronograf/issues/new)!

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

* Chronograf works with go 1.7.3, npm 3.10.7 and node v6.6.0. Additional version support of these projects will be implemented soon, but these are the only supported versions to date.
* Chronograf requires [Kapacitor](https://github.com/influxdata/kapacitor) 1.1 to create and store alerts.

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
