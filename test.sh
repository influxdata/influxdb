#!/usr/bin/env bash

set -e

cd `dirname $0`
. exports.sh

if [ "x`uname`" == "xLinux" ]; then
    pushd src/query
    ./build_parser.sh
    if ! ./test_memory_leaks.sh; then
	echo "ERROR: memory leak detected"
	exit 1
    fi
    popd
fi

go get launchpad.net/gocheck

./build.sh

go test -v query
go test -v protocol
