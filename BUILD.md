# Build InfluxDB

To build the commandline interface and the daemon just run ``.

## Alpine

```
$ docker run --rm -ti -v $(pwd):/usr/local/src/github.com/influxdata/influxdb/ --workdir /usr/local/src/github.com/influxdata/influxdb/ qnib/alpn-go-dev ./build-qnib.sh
fatal: no tag exactly matches 'cbe96a6f5606fe473b9b4f3b7f6ea440495b5fd4'
> go build -o ./bin/influx_v1.0.2-dirty-190_alpine
> go build -o ./bin/influxd_v1.0.2-dirty-190_alpine
```
