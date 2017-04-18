FROM golang:1.4.3
MAINTAINER Jonathan A. Sternberg <jonathan@influxdb.com>

RUN go get github.com/sparrc/gdm
COPY . /go/src/github.com/influxdata/influxdb
WORKDIR /go/src/github.com/influxdata/influxdb
RUN gdm restore
RUN go install -v github.com/influxdata/influxdb/cmd/...
RUN mkdir -p /etc/influxdb && \
    INFLUXDB_DATA_DIR=/var/lib/influxdb/data \
    INFLUXDB_META_DIR=/var/lib/influxdb/meta \
    INFLUXDB_DATA_WAL_DIR=/var/lib/influxdb/wal \
    influxd config > /etc/influxdb/influxdb.conf

EXPOSE 8083 8086

VOLUME /var/lib/influxdb

COPY docker-entrypoint.sh /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
CMD ["influxd"]
