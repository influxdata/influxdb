package storage

//go:generate protoc -I$GOPATH/src/github.com/influxdata/influxdb/vendor -I. --gogofaster_out=. source.proto
