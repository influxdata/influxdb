#!/bin/bash

if [ -e build ]; then
  rm -rf build/*
else
  mkdir build
fi

export INFLUXDB_VERSION=`cat VERSION`
sed -i.bak "s/var version = \"dev\"/var version = \"$INFLUXDB_VERSION\"/" src/server/server.go
export GOPATH=`pwd`
go build src/server/server.go
sed -i.bak "s/var version = \"$INFLUXDB_VERSION\"/var version = \"dev\"/" src/server/server.go
rm src/server/server.go.bak
# mv server build/influxdb

# cp config.json.sample build/config.json

# cp -R src/admin/site/ build/admin/

# tar -czf influxdb-`cat VERSION`.tar.gz build/*
