#!/usr/bin/env bash

set -e

cd `dirname $0`
. exports.sh

function print_usage {
    echo "$0 [-o regex] [-p package_name] -- <test arguments>"
    echo "  -o|--only:     Run the test that matches the given regex"
    echo "  --no-valgrind: Skip the valgrind memory leak test"
    echo "  -p|--packages: Run the test in the given packages only"
    echo "  -b|--benchmarks: Run benchmarks"
    echo "  -v|--verbose:    Prints verbose output"
    echo "  -h|--help:       Prints this help message"
}

while [ $# -ne 0 ]; do
    case "$1" in
        -h|--help) print_usage; exit 1; shift;;
        -v|--verbose) verbose=on; shift;;
        -o|--only) regex=$2; shift 2;;
        --no-valgrind) valgrind=no; shift;;
        -p|--packages) test_packages="$test_packages $2"; shift 2;;
        -b|--benchmarks) gocheck_args="$gocheck_args -gocheck.b"; shift;;
        --) shift ; break ;;
        *) echo "Internal error!" ; exit 1 ;;
    esac
done

go fmt $packages || echo "Cannot format code"

./build.sh

if [ "x`uname`" == "xLinux" -a "x$valgrind" != "xno" ]; then
    pushd src/parser
    if ! ./test_memory_leaks.sh; then
        echo "ERROR: memory leak detected"
        exit 1
    fi
    popd
fi

[ "x$test_packages" == "x" ] && test_packages="$packages"
echo "Running tests for packages: $test_packages"

[ "x$regex" != "x" ] && gocheck_args="$gocheck_args -gocheck.f $regex"
[ "x$verbose" == "xon" ] && gocheck_args="$gocheck_args -v -gocheck.v"

ulimit -n 2048 || echo could not change ulimit

function notify_failure {
    if ! which notify-send > /dev/null 2>&1; then
        return
    fi

    notify-send Influxdb "Package $1 test failed"
}

function notify_end {
    if ! which notify-send > /dev/null 2>&1; then
        return
    fi

    notify-send Influxdb "Test script finished"
}

for i in $test_packages; do
    if go version | grep go1.2 > /dev/null 2>&1; then
        go test -coverprofile /tmp/influxdb.${i/\//.}.coverage $i $gocheck_args $@ || notify_failure $i
    else
        go test $i $gocheck_args $@ || notify_failure $i
    fi
done

notify_end
