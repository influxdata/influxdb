# Fuzzing InfluxDB

## Local fuzzing

For local fuzzing, install [go-fuzz](https://github.com/dvyukov/go-fuzz):

```
$ go get -u github.com/dvyukov/go-fuzz/go-fuzz github.com/dvyukov/go-fuzz/go-fuzz-build
```

For writing fuzz tests, see the [go-fuzz README](https://github.com/dvyukov/go-fuzz).

Below is an example of building and running a fuzz test.
In this case, the test is located at `./jsonweb/fuzz.go`.

```
$ cd go/src/github.com/influxdata/influxdb/jsonweb
$ go-fuzz-build github.com/influxdata/influxdb/v2/jsonweb
$ go-fuzz -bin jsonweb-fuzz.zip
```
