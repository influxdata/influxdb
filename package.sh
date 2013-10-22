#!/bin/bash

if [ -e build ]; then
  rm -rf build/*
else
  mkdir build
fi

go build src/server/server.go
mv server build/influxdb

cp config.json.sample build/config.json

cp -R src/admin/site/ build/admin/

tar -czf influxdb-`cat VERSION`.tar.gz build/*
