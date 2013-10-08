#!/usr/bin/env bash

. ./exports.sh

go get code.google.com/p/goprotobuf/proto

pushd src
packages=$(ls -d * | egrep -v 'google|launchpad|github' | tr '\n' ' ')
popd

echo "packages: go build $packages"

go build $packages
