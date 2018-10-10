#!/bin/bash

set -e

export GO111MODULE=on
go mod tidy

if ! git --no-pager diff --exit-code -- go.mod go.sum; then
  >&2 echo "modules are not tidy, please run 'go mod tidy'"
  exit 1
fi
