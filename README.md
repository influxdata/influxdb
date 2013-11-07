InfluxDB [![Build Status](https://travis-ci.org/influxdb/influxdb.png?branch=master)](https://travis-ci.org/influxdb/influxdb)
=========

InfluxDB is an open source **distributed time series database** with **no external dependencies**. It's useful for recording metrics, events, and performing analytics.  

It has a built-in HTTP API so you don't have to write any server side code to get up and running. 

InfluxDB is designed to be scalable, simple to install and manage, and fast to get data in and out.

It aims to answer queries in real-time. That means every data point is indexed as it comes in and is immediately available in queries that should return in < 100ms. 

## Quickstart

* Understand the [design goals and motivations of the project](http://influxdb.org/overview/).
* Follow the [getting started guide](http://influxdb.org/docs/) to find out how to install InfluxDB, start writing data, and issue queries - in just a few minutes.
* See the [list of libraries for different langauges](http://influxdb.org/docs/libraries/javascript.html), or check out the [HTTP API documentation to start writing a library for your favorite language](http://influxdb.org/docs/api/http.html).

## Building

### Mac OS X

- Install the build dependencies of the project: 

``` shell 
brew install protobuf bison flex leveldb go hg bzr
```

- Run `./test.sh`. This will build the server and run the tests.

Note: If you are on Mac OS X Mavericks, you might want to try to install go using `brew install go --devel`

### Linux

- You need to [get Go from Google Code](http://code.google.com/p/go/downloads/list).
- Ensure `go` is on your `PATH`.
- If you're on a Red Hat-based distro: 

``` bash
sudo yum install hg bzr protobuf-compiler flex bison
```

- If you're on a Debian-based distro:
``` 
sudo apt-get install mercurial bzr protobuf-compiler flex bison valgrind
```

- Run `./test.sh`. This will build the server and run the tests.
