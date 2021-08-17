#!/usr/bin/env bash

# This script clones the openapi repo and extracts the OSS swagger.json for the
# specified commit.

set -e

declare -r SCRIPT_DIR=$(cd $(dirname ${0}) >/dev/null 2>&1 && pwd)
declare -r ROOT_DIR=$(dirname ${SCRIPT_DIR})
declare -r STATIC_DIR="$ROOT_DIR/static"

# Pins the swagger that will be downloaded to a specific commit
declare -r OPENAPI_SHA=3e4049752b7c59c6d9a5d5f76e8d73e61d1d66b1

# Don't do a shallow clone since the commit we want might be several commits
# back; but do only clone the main branch.
git clone https://github.com/influxdata/openapi.git --single-branch
mkdir -p "$STATIC_DIR/data"
cd openapi && git checkout ${OPENAPI_SHA} --quiet && cp contracts/oss.json "$STATIC_DIR/data/swagger.json"
cd ../ && rm -rf openapi
