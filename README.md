# Chronograf

Chronograf is an open-source web application written in Go and React.js that provides the tools to visualize your monitoring data and easily create alerting and automation rules.

<p align="left">
  <img src="https://github.com/influxdata/chronograf/blob/master/docs/images/overview-readme.png"/>
</p>

## Features

### Host List

* List and sort hosts
* View general CPU and load stats
* View and access dashboard templates for configured apps

### Dashboard Templates

Chronograf's [pre-canned dashboards](https://github.com/influxdata/chronograf/tree/master/canned) for the supported [Telegraf](https://github.com/influxdata/telegraf) input plugins.
Currently, Chronograf offers dashboard templates for the following Telegraf input plugins:

* [Apache](https://github.com/influxdata/telegraf/blob/master/plugins/inputs/apache)
* [Consul](https://github.com/influxdata/telegraf/blob/master/plugins/inputs/consul)
* [Docker](https://github.com/influxdata/telegraf/blob/master/plugins/inputs/docker)
* [Elastic](https://github.com/influxdata/telegraf/blob/master/plugins/inputs/elasticsearch)
* etcd
* [HAProxy](https://github.com/influxdata/telegraf/blob/master/plugins/inputs/haproxy)
* IIS
* [InfluxDB](https://github.com/influxdata/telegraf/blob/master/plugins/inputs/influxdb)
* [Kubernetes](https://github.com/influxdata/telegraf/blob/master/plugins/inputs/kubernetes)
* [Memcached](https://github.com/influxdata/telegraf/blob/master/plugins/inputs/memcached)
* [Mesos](https://github.com/influxdata/telegraf/blob/master/plugins/inputs/mesos)
* [MongoDB](https://github.com/influxdata/telegraf/blob/master/plugins/inputs/mongodb)
* [MySQL](https://github.com/influxdata/telegraf/blob/master/plugins/inputs/mysql)
* Network
* [NGINX](https://github.com/influxdata/telegraf/blob/master/plugins/inputs/nginx)
* [NSQ](https://github.com/influxdata/telegraf/blob/master/plugins/inputs/nsq)
* [Ping](https://github.com/influxdata/telegraf/blob/master/plugins/inputs/ping)
* [PostgreSQL](https://github.com/influxdata/telegraf/blob/master/plugins/inputs/postgresql)
* Processes
* [RabbitMQ](https://github.com/influxdata/telegraf/blob/master/plugins/inputs/rabbitmq)
* [Redis](https://github.com/influxdata/telegraf/blob/master/plugins/inputs/redis)
* [Riak](https://github.com/influxdata/telegraf/blob/master/plugins/inputs/riak)
* [System](https://github.com/influxdata/telegraf/blob/master/plugins/inputs/system/SYSTEM_README.md)
    * [CPU](https://github.com/influxdata/telegraf/blob/master/plugins/inputs/system/CPU_README.md)
    * [Disk](https://github.com/influxdata/telegraf/blob/master/plugins/inputs/system/DISK_README.md)
    * [DiskIO](https://github.com/influxdata/telegraf/blob/master/plugins/inputs/system/disk.go#L136)
    * [Memory](https://github.com/influxdata/telegraf/blob/master/plugins/inputs/system/MEM_README.md)
    * [Net](https://github.com/influxdata/telegraf/blob/master/plugins/inputs/system/net.go)
    * [Netstat](https://github.com/influxdata/telegraf/blob/master/plugins/inputs/system/NETSTAT_README.md)
    * [Processes](https://github.com/influxdata/telegraf/blob/master/plugins/inputs/system/PROCESSES_README.md)
    * [Procstat](https://github.com/influxdata/telegraf/blob/master/plugins/inputs/procstat/README.md)
* [Varnish](https://github.com/influxdata/telegraf/blob/master/plugins/inputs/varnish)
* [Windows Performance Counters](https://github.com/influxdata/telegraf/blob/master/plugins/inputs/win_perf_counters)

> Note: If a `telegraf` instance isn't running the `system` and `cpu` plugins the canned dashboards from that instance won't be generated.

### Data Explorer

Chronograf's graphing tool that allows you to dig in and create personalized visualizations of your data.

* Generate [InfluxQL](https://docs.influxdata.com/influxdb/latest/query_language/) statements with the query builder
* Generate and edit [InfluxQL](https://docs.influxdata.com/influxdb/latest/query_language/) statements with the raw query editor
* Create visualizations and view query results in tabular format

### Dashboards

While there is an API and presentation layer for dashboards released in version 1.2.0-beta1+, it is not recommended that you try to use Chronograf as a general purpose dashboard solution. The visualization around editing is under way and will be in a future release. Meanwhile, if you would like to try it out you can use `curl` or other HTTP tools to push dashboard definitions directly to the API. If you do so, they should be shown when selected in the application.

Example:
```
curl -X POST -H "Content-Type: application/json" -d '{
    "cells": [
        {
            "queries": [
                {
                    "label": "%",
                    "query": "SELECT mean(\"usage_user\") AS \"usage_user\" FROM \"cpu\"",
                    "wheres": [],
                    "groupbys": []
                }
            ],
            "type": "line"
        }
    ],
    "name": "dashboard name"
}' "http://localhost:8888/chronograf/v1/dashboards"
```

### Kapacitor UI

A UI for [Kapacitor](https://github.com/influxdata/kapacitor) alert creation and alert tracking.

* Simply generate threshold, relative, and deadman alerts
* Preview data and alert boundaries while creating an alert
* Configure alert destinations - Currently, Chronograf supports sending alerts to:
  * [Alerta](https://docs.influxdata.com/kapacitor/latest/nodes/alert_node/#alerta)
  * [Exec](https://docs.influxdata.com/kapacitor/latest/nodes/alert_node/#exec)
  * [HipChat](https://docs.influxdata.com/kapacitor/latest/nodes/alert_node/#hipchat)
  * [HTTP/Post](https://docs.influxdata.com/kapacitor/latest/nodes/alert_node/#post)
  * [OpsGenie](https://docs.influxdata.com/kapacitor/latest/nodes/alert_node/#opsgenie)
  * [PagerDuty](https://docs.influxdata.com/kapacitor/latest/nodes/alert_node/#pagerduty)
  * [Sensu](https://docs.influxdata.com/kapacitor/latest/nodes/alert_node/#sensu)
  * [Slack](https://docs.influxdata.com/kapacitor/latest/nodes/alert_node/#slack)
  * [SMTP/Email](https://docs.influxdata.com/kapacitor/latest/nodes/alert_node/#email)
  * [Talk](https://docs.influxdata.com/kapacitor/latest/nodes/alert_node/#talk)
  * [Telegram](https://docs.influxdata.com/kapacitor/latest/nodes/alert_node/#telegram)
  * [TCP](https://docs.influxdata.com/kapacitor/latest/nodes/alert_node/#tcp)
  * [VictorOps](https://docs.influxdata.com/kapacitor/latest/nodes/alert_node/#victorops)
* View all active alerts at a glance on the alerting dashboard
* Enable and disable existing alert rules with the check of a box

### User and Query Management

Manage users, roles, permissions for [OSS InfluxDB](https://github.com/influxdata/influxdb) and InfluxData's [Enterprise](https://docs.influxdata.com/enterprise/v1.2/) product.
View actively running queries and stop expensive queries on the Query Management page.

These features are new in Chronograf version 1.2.0-beta5. We recommend using them in a non-production environment only. Should you come across any bugs or unexpected behavior please open [an issue](https://github.com/influxdata/chronograf/issues/new). We appreciate the feedback as we work to finalize and improve the user and query management features!

### TLS/HTTPS Support
See [Chronograf with TLS](https://github.com/influxdata/chronograf/blob/master/docs/tls.md) for more information.

### OAuth Login
See [Chronograf with OAuth 2.0](https://github.com/influxdata/chronograf/blob/master/docs/auth.md) for more information.

### Advanced Routing
Change the default root path of the Chronograf server with the `--basepath` option.

## Versions

Chronograf v1.2.0-beta5 is a beta release.
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

By default, chronograf runs on port `8888`.


### With Docker
To get started right away with Docker, you can pull down our latest alpha:

```sh
docker pull quay.io/influxdb/chronograf:latest
```

### From Source

* Chronograf works with go 1.7.x, node 6.x/7.x, and yarn 0.18+.
* Chronograf requires [Kapacitor](https://github.com/influxdata/kapacitor) 1.1.x+ to create and store alerts.

1. [Install Go](https://golang.org/doc/install)
1. [Install Node and NPM](https://nodejs.org/en/download/)
1. [Install yarn](https://yarnpkg.com/docs/install)
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
