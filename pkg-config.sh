#!/usr/bin/env bash

tmpdir=$(mktemp -d)
trap "{ rm -rf ${tmpdir}; }" EXIT

# "go build" can be noisy, and when Go invokes pkg-config (by calling this script) it will merge stdout and stderr.
# Discard any output unless "go build" terminates with an error.
go build -o ${tmpdir}/pkg-config github.com/influxdata/pkg-config &> ${tmpdir}/go_build_output
if [ "$?" -ne 0 ]; then
    cat ${tmpdir}/go_build_output 1>&2
    exit 1
fi

${tmpdir}/pkg-config "$@"
