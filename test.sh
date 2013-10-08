#!/usr/bin/env bash

set -e

cd `dirname $0`
. exports.sh

pushd src/query
./build_parser.sh
if [ "x`uname`" == "xLinux" ]; then
    if ! ./test_memory_leaks.sh; then
        echo "ERROR: memory leak detected"
        exit 1
    fi
fi
popd

go get launchpad.net/gocheck

./build.sh

go test -v query
go test -v protocol
