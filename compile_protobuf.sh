#!/usr/bin/env bash

. ./exports.sh

go get code.google.com/p/goprotobuf/proto \
    code.google.com/p/goprotobuf/protoc-gen-go

rm src/protocol/*.pb.go
PATH=bin:$PATH protoc --go_out=. src/protocol/*.proto
