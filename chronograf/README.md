# Chronograf

Chronograf is an open-source web application written in Go and React.js that
provides the tools to visualize your monitoring data and easily create alerting
and automation rules.

<p align="left">
  <img src="https://github.com/influxdata/chronograf/blob/master/docs/images/overview-readme.png"/>
</p>

## Features

### Host List

* List and sort hosts
* View general CPU and load stats
* View and access dashboard templates for configured apps

### Dashboard Templates

Chronograf's
[pre-canned dashboards](https://github.com/influxdata/chronograf/tree/master/canned)
for the supported [Telegraf](https://github.com/influxdata/telegraf) input
plugins. Currently, Chronograf offers dashboard templates for the following
Telegraf input plugins:

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
* [PHPfpm](https://github.com/influxdata/telegraf/blob/master/plugins/inputs/phpfpm)
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

> Note: If a `telegraf` instance isn't running the `system` and `cpu` plugins
> the canned dashboards from that instance won't be generated.

### Data Explorer

Chronograf's graphing tool that allows you to dig in and create personalized
visualizations of your data.

* Generate and edit
  [InfluxQL](https://docs.influxdata.com/influxdb/latest/query_language/)
  statements with the query editor
* Use Chronograf's query templates to easily explore your data
* Create visualizations and view query results in tabular format

### Dashboards

Create and edit customized dashboards. The dashboards support several
visualization types including line graphs, stacked graphs, step plots, single
statistic graphs, and line-single-statistic graphs.

Use Chronograf's template variables to easily adjust the data that appear in
your graphs and gain deeper insight into your data.

### Kapacitor UI

A UI for [Kapacitor](https://github.com/influxdata/kapacitor) alert creation and
alert tracking.

* Simply generate threshold, relative, and deadman alerts
* Preview data and alert boundaries while creating an alert
* Configure alert destinations - Currently, Chronograf supports sending alerts
  to:
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
* Configure multiple Kapacitor instances per InfluxDB source

### User and Query Management

Manage users, roles, permissions for
[OSS InfluxDB](https://github.com/influxdata/influxdb) and InfluxData's
[Enterprise](https://docs.influxdata.com/enterprise/v1.2/) product. View
actively running queries and stop expensive queries on the Query Management
page.

### TLS/HTTPS Support

See
[Chronograf with TLS](https://github.com/influxdata/chronograf/blob/master/docs/tls.md)
for more information.

### OAuth Login

See
[Chronograf with OAuth 2.0](https://github.com/influxdata/chronograf/blob/master/docs/auth.md)
for more information.

### Advanced Routing

Change the default root path of the Chronograf server with the `--basepath`
option.

## Versions

The most recent version of Chronograf is
[v1.5.0.0](https://www.influxdata.com/downloads/).

Spotted a bug or have a feature request? Please open
[an issue](https://github.com/influxdata/chronograf/issues/new)!

### Known Issues

The Chronograf team has identified and is working on the following issues:

* Chronograf requires users to run Telegraf's
  [CPU](https://github.com/influxdata/telegraf/blob/master/plugins/inputs/system/CPU_README.md)
  and
  [system](https://github.com/influxdata/telegraf/blob/master/plugins/inputs/system/SYSTEM_README.md)
  plugins to ensure that all Apps appear on the
  [HOST LIST](https://github.com/influxdata/chronograf/blob/master/docs/GETTING_STARTED.md#host-list)
  page.

## Installation

Check out the
[INSTALLATION](https://docs.influxdata.com/chronograf/v1.4/introduction/installation/)
guide to get up and running with Chronograf with as little configuration and
code as possible.

We recommend installing Chronograf using one of the
[pre-built packages](https://influxdata.com/downloads/#chronograf). Then start
Chronograf using:

* `service chronograf start` if you have installed Chronograf using an official
  Debian or RPM package.
* `systemctl start chronograf` if you have installed Chronograf using an
  official Debian or RPM package, and are running a distro with `systemd`. For
  example, Ubuntu 15 or later.
* `$GOPATH/bin/chronograf` if you have built Chronograf from source.

By default, chronograf runs on port `8888`.

### With Docker

To get started right away with Docker, you can pull down our latest release:

```sh
docker pull chronograf:1.5.0.0
```

### From Source

* Chronograf works with go 1.10+, node 8.x, and yarn 1.5+.
* Chronograf requires [Kapacitor](https://github.com/influxdata/kapacitor)
  1.2.x+ to create and store alerts.

1. [Install Go](https://golang.org/doc/install)
1. [Install Node and NPM](https://nodejs.org/en/download/)
1. [Install yarn](https://yarnpkg.com/docs/install)
1. [Setup your GOPATH](https://golang.org/doc/code.html#GOPATH)
1. Build the Chronograf package:
    ```bash
    go get github.com/influxdata/chronograf
    cd $GOPATH/src/github.com/influxdata/chronograf
    make
    ```
1. Install the newly built Chronograf package:
    ```bash
    go install github.com/influxdata/chronograf/cmd/chronograf
    ```

## Documentation

[Getting Started](https://docs.influxdata.com/chronograf/v1.4/introduction/getting-started/)
will get you up and running with Chronograf with as little configuration and
code as possible. See our
[guides](https://docs.influxdata.com/chronograf/v1.4/guides/) to get familiar
with Chronograf's main features.

Documentation for Telegraf, InfluxDB, and Kapacitor are available at
https://docs.influxdata.com/.

Chronograf uses
[swagger](https://swagger.io/specification://swagger.io/specification/) to
document its REST interfaces. To reach the documentation, run the server and go
to the `/docs` for example at http://localhost:8888/docs

The swagger JSON document is in `server/swagger.json`

## Contributing

Please see the [contributing guide](CONTRIBUTING.md) for details on contributing
to Chronograf.
