#!/usr/bin/env bash

set -e

cd `dirname $0`
. exports.sh

valgrind_result=0
pushd src/query
./build_parser.sh
if ! ./test_memory_leaks.sh; then
    echo "ERROR: memory leak detected"
    exit 1
fi
popd

go get launchpad.net/gocheck

./build.sh

go test -v query
go test -v protocol
