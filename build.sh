#!/usr/bin/env bash

. ./exports.sh

go get code.google.com/p/goprotobuf/proto

echo "packages: go build $packages"

go build $packages
