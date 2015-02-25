# InfluxDB [![Build Status](https://travis-ci.org/influxdb/influxdb.png?branch=master)](https://travis-ci.org/influxdb/influxdb)

## An Open-Source, Distributed, Time Series Database

> InfluxDB v0.9.0 is now in the RC phase. If you're building a new project,
> please build against `master` or the most current RC instead of using v0.8.8.

InfluxDB is an open source **distributed time series database** with
**no external dependencies**. It's useful for recording metrics,
events, and performing analytics.

## Features

* Built-in HTTP API so you don't have to write any server side code to get up and running.
* Clustering is supported out of the box, so that you can scale horizontally to handle your data.
* Simple to install and manage, and fast to get data in and out.
* It aims to answer queries in real-time. That means every data point is
  indexed as it comes in and is immediately available in queries that
  should return in < 100ms.

## Getting Started



### Building

You don't need to build the project to use it - you can use any of our
[pre-built packages](http://influxdb.com/download/) to install InfluxDB. That's
the recommended way to get it running. However, if you want to contribute to the core of InfluxDB, you'll need to build.
For those adventurous enough, you can
[follow along on our docs](http://github.com/influxdb/influxdb/blob/master/CONTRIBUTING.md).

### Creating your first database

```JSON
curl -G 'http://localhost:8086/query' \
    --data-urlencode "q=CREATE DATABASE mydb"
```
### Setting up the database's retention policy

```JSON
curl -G 'http://localhost:8086/query' \
    --data-urlencode "q=CREATE RETENTION POLICY mypolicy \
    ON mydb DURATION 7d REPLICATION 1 DEFAULT"
```
### Insert some data
```JSON
curl -H "Content-Type: application/json" http://localhost:8086/write -d '
{
    "database": "mydb",
    "retentionPolicy": "mypolicy",
    "points": [
        {
            "name": "cpu",
             "tags": {
                 "region":"uswest",
                 "host": "server01"},
             "fields": {"value": 100}
         }
      ]
}'
```
### Query for the data
```JSON
curl -G http://localhost:8086/query?pretty=true \
--data-urlencode "db=mydb" --data-urlencode "q=SELECT * FROM cpu"
```
## Helpful Links

* Understand the [design goals and motivations of the project](http://influxdb.com/docs/v0.9/introduction/overview.html).
* Follow the [getting started guide](http://influxdb.com/docs/v0.9/introduction/getting_started.html) to find out how to install InfluxDB, start writing data, and issue queries - in just a few minutes.
* See the  [HTTP API documentation to start writing a library for your favorite language](http://influxdb.com/docs/v0.9/concepts/reading_and_writing_data.html).

