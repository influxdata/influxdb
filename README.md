InfluxDB [![Build Status](https://travis-ci.org/influxdb/influxdb.png?branch=master)](https://travis-ci.org/influxdb/influxdb)
=========

InfluxDB is an open source distributed time series database with no external dependencies. It's useful for metrics, events, and analytics with a built in HTTP API so you don't have to write any server side code to get up and running. InfluxDB is designed to answer queries in real-time. That means every data point is indexed as it comes in and is immediately available in queries that should return in < 100ms. It's designed to be scalabe, simple to install and manage, and fast to get data in and out.

Read an [overview of the design goals and reasons for the project](http://influxdb.org/overview/).

Check out the [getting started guide](http://influxdb.org/docs/) to read about how to install InfluxDB, start writing data, and issue queries in just a few minutes.

See the [list of libraries for different langauges](http://influxdb.org/docs/libraries/javascript.html). Or see the [HTTP API documentation to start writing a library for your favorite language](http://influxdb.org/docs/api/http.html).

## Building

### Mac OS

- install the build dependencies of the project `brew install protobuf bison flex leveldb go hg bzr`
- Run `./test.sh`

The second step should build the server and run the tests.

Note: if you're on Mac OS Maverick, you might want to try to install go using `brew install go --devel`

### Linux

- You need to get go from [here](http://code.google.com/p/go/downloads/list)
- Make sure go is on your PATH
- If you're on a redhat based distro `sudo yum install hg bzr protobuf-compiler flex bison`
- If you're on a debian based distro `sudo apt-get install hg bzr protobuf-compiler flex bison`
- Run `./test.sh`

The last step should build the server and run the tests.
