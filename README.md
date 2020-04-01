# Warp 10 Platform [![Tweet](https://img.shields.io/twitter/url/http/shields.io.svg?style=social)](https://twitter.com/intent/tweet?text=Get%20The%20Most%20Advanced%20Time%20Series%20Platform&url=https://warp10.io/download&via=warp10io&hashtags=tsdb,database,timeseries,opensource)
[![Build Status](https://www.travis-ci.org/senx/warp10-platform.svg?branch=master)](https://www.travis-ci.org/senx/warp10-platform)
[![Download](https://api.bintray.com/packages/senx/generic/warp10/images/download.svg)](https://bintray.com/senx/generic/warp10/_latestVersion)

<p align="center"><img src="https://blog.senx.io/wp-content/uploads/2018/10/warp10bySenx.png" alt="Warp 10 Logo" width="50%"></p>

## Introduction

Warp 10 is an Open Source Geo Time Series Platform designed to handle data coming from sensors, monitoring systems and the Internet of Things.

Geo Time Series extend the notion of Time Series by merging the sequence of sensor readings with the sequence of sensor locations. If your data have no location information, Warp 10 will handle them as regular Time Series.

## Features

The Warp 10 Platform provides a rich set of features to simplify your work around sensor data.

* Warp 10 Storage Engine, our collection and storage layer, a Geo Time Series Database
* WarpScript, a language dedicated to sensor data analysis with more than 1000 functions and extension capabilities
* Plasma and Mobius, streaming engines allowing to cascade the Warp 10 Platform with Complex Event Processing solutions and to build dynamic dashboards
* Runner, a system for scheduling WarpScript program executions on the server side
* [Sensision](https://github.com/senx/sensision), a framework for exposing metrics and pushing them into Warp 10
* Standalone version running on a [Raspberry Pi](https://blog.senx.io/warp-10-raspberry-bench-for-industrial-iot/) as well as on a beefy server, with no external dependencies
* Replication and sharding of standalone instances using the Datalog mechanism
* Distributed version based on Hadoop HBase for the most demanding environments
* Integration with [Pig](https://github.com/senx/warp10-pig), [Spark](https://github.com/senx/warp10-spark2), [Flink](https://github.com/senx/warp10-flink), [NiFi](https://github.com/senx/nifi-warp10-processor), [Kafka Streams](https://github.com/senx/warp10-plugin-kstreams) and [Storm](https://github.com/senx/warp10-storm) for batch and streaming analysis.

## Getting Help

The team has put lots of efforts into the [documentation](https://warp10.io/) of the Warp 10 Platform, there are still some areas which may need improving, so we count on you to raise the overall quality.

We understand that discovering all the features of the Warp 10 Platform at once can be intimidating, that's why we've put together a [Google Group](https://groups.google.com/forum/#!forum/warp10-users) we recommend you subscribe to. You can also use [StackOverflow](https://stackoverflow.com/) [warp10](https://stackoverflow.com/search?q=warp10) and [warpscript](https://stackoverflow.com/search?q=warpscript) tags.

Our goal is to build a large community of users to move our platform into territories we haven't explored yet and to make Warp 10 and WarpScript the standards for sensor data and the IoT.

We also suggest you subscribe to our Twitter account [@warp10io](https://twitter.com/warp10io).

## Commercial Support

Should you need commercial support for your projects, [SenX](https://senx.io/) offers support plans which will give you access to the core team developing the platform.

Don't hesitate to contact us at [sales@senx.io](mailto:sales@senx.io) for all your enquiries.

#### Trademarks

Warp 10, WarpScript, WarpFleet, Geo Time Series and SenX are trademarks of SenX S.A.S.
