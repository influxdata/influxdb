# Fuzzing InfluxDB

## Continuous fuzzing

The InfluxDB repository contains testing targets for InfluxDB's integration with
[OSS-Fuzz](https://google.github.io/oss-fuzz/).

The build scripts for this integration are contained [in the OSS-Fuzz repo](https://github.com/google/oss-fuzz/tree/master/projects/influxdb).

## Local fuzzing

For local fuzzing, install [go-fuzz](https://github.com/dvyukov/go-fuzz):

```
$ go get -u github.com/dvyukov/go-fuzz/go-fuzz github.com/dvyukov/go-fuzz/go-fuzz-build
```

Below is an example of building and running a fuzz test.
In this case, the test is located at `./jsonweb/fuzz.go`.

```
$ cd go/src/github.com/influxdata/influxdb/jsonweb
$ go-fuzz-build github.com/influxdata/influxdb/v2/jsonweb
$ go-fuzz -bin jsonweb-fuzz.zip
```
