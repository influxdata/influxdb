#!/usr/bin/env bash

cd `dirname $0`
. exports.sh

go get launchpad.net/gocheck

go test -v query
