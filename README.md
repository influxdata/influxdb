# Chronograf

## An Open-Source UI for Monitoring and Alerting your Infrastructure
Chronograf is an open-source web application written in go and react.js that is designed to simply and easily visualize your monitoring data and provide tools to create alerting and automation rules. 

## Features
* High level infrastructure view and search
* Application specific monitoring for:
  * System stats
  * InfluxDB
  * Docker Containers
  * Kuberentes
  * Redis
  * NSQ
  * MySQL
  * PostgreSQL
* Kapacitor alert creation and tracking

## Installation

We recommend installing Chrongraf using one of the [pre-built packages](https://influxdata.com/downloads/#chronograf). Then start Chronograf using:

* `service chronograf start` if you have installed Chronograf using an official Debian or RPM package.
* `systemctl start chronograf` if you have installed Chronograf using an official Debian or RPM package, and are running a distro with `systemd`. For example, Ubuntu 15 or later.
* `$GOPATH/bin/chronograf` if you have built Chronograf from source.

## Builds

* Chronograf works with go 1.7.3, npm 3.10.7 and node v6.6.0. Additional version support of these projects will be implemented soon, but these are the only supported versions to date.
* Chronograf requires Kapacitor 1.1 to create and store alerts.
* To build assets and the go server, run `make`.
* To run server either `./chronograf --port 8888` or `make run`

## Getting Started
See the [getting started](https://github.com/influxdata/chronograf/blob/master/docs/GETTING_STARTED.md) guide for setup instructions for chronograf and the other components of the [TICK stack](https://www.influxdata.com/get-started/what-is-the-tick-stack/)

## Contributing

Please see the [contributing guide](CONTRIBUTING.md) for details on contributing to Chronograf.


