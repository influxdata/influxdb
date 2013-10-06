#!/usr/bin/env bash

cd `dirname $0`
. exports.sh

go get launchpad.net/gocheck

./build.sh

go test -v query
go test -v protocol
