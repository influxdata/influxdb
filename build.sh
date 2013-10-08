#!/usr/bin/env bash

. ./exports.sh

go get code.google.com/p/goprotobuf/proto
go get github.com/goraft/raft
go get github.com/gorilla/mux


echo "packages: go build $packages"

go build $packages
