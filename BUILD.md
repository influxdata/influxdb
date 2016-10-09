# Build InfluxDB

To build the commandline interface and the daemon just run ``.

## Make Alpine/Linux version

```
$ make linux alpine
docker run --rm -ti -v /Users/kniepbert/src/github.com/ChristianKniep/influxdb:/usr/local/src/github.com/influxdata/influxdb/ --workdir /usr/local/src/github.com/influxdata/influxdb/ qnib/golang ./build-qnib.sh
fatal: no tag exactly matches '3206dac290735d7c11a4d8ebe1c8f59d652f94bc'
> go build -o ./bin/influx_v1.0.2-dirty-193_linux
> go build -o ./bin/influxd_v1.0.2-dirty-193_linux
docker run --rm -ti -v /Users/kniepbert/src/github.com/ChristianKniep/influxdb:/usr/local/src/github.com/influxdata/influxdb/ --workdir /usr/local/src/github.com/influxdata/influxdb/ qnib/alpn-go-dev ./build-qnib.sh
fatal: no tag exactly matches '3206dac290735d7c11a4d8ebe1c8f59d652f94bc'
> go build -o ./bin/influx_v1.0.2-dirty-193_alpine
> go build -o ./bin/influxd_v1.0.2-dirty-193_alpine
```
