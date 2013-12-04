InfluxDB [![Build Status](https://travis-ci.org/influxdb/influxdb.png?branch=master)](https://travis-ci.org/influxdb/influxdb)
=========

InfluxDB is an open source **distributed time series database** with **no external dependencies**. It's useful for recording metrics, events, and performing analytics.

It has a built-in HTTP API so you don't have to write any server side code to get up and running.

InfluxDB is designed to be scalable, simple to install and manage, and fast to get data in and out.

It aims to answer queries in real-time. That means every data point is indexed as it comes in and is immediately available in queries that should return in < 100ms.

## Quickstart

* Understand the [design goals and motivations of the project](http://influxdb.org/overview/).
* Follow the [getting started guide](http://influxdb.org/docs/) to find out how to install InfluxDB, start writing data, and issue queries - in just a few minutes.
* See the [list of libraries for different languages](http://influxdb.org/docs/libraries/javascript.html), or check out the [HTTP API documentation to start writing a library for your favorite language](http://influxdb.org/docs/api/http.html).

## Building

You don't need to build the project to use it. Pre-built [binaries and instructions to install InfluxDB are here](http://influxdb.org/download/). That's the recommended way to get it running. However, if you want to contribute to the core of InfluxDB, you'll need to build. For those adventurous enough, you can [follow along on our docs](http://influxdb.org/docs/getting_started/building.html).

