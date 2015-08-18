#!/bin/bash

docker build -f Dockerfile_test_ubuntu32 -t ubuntu-32-influxdb-test .
docker run -v $(pwd):/root/go/src/github.com/influxdb/influxdb -t ubuntu-32-influxdb-test bash -c "cd /root/go/src/github.com/influxdb/influxdb && go get -t -d -v ./... && go build -v ./... && go test -v ./..."
