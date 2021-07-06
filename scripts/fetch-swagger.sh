#!/bin/sh

# This script clones the openapi repo and extracts the OSS swagger.json for the
# specified commit.

set -e

# Pins the swagger that will be downloaded to a specific commit
declare -r OPENAPI_SHA=c87a08f832e79bc338a37509028641cda9e1875b

# Don't do a shallow clone since the commit we want might be several commits
# back; but do only clone the main branch.
git clone https://github.com/influxdata/openapi.git --single-branch
cd openapi && git checkout ${OPENAPI_SHA} --quiet && cp contracts/oss.json ../data/swagger.json
cd ../ && rm -rf openapi
