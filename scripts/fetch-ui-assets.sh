#!/usr/bin/env bash

declare -r SCRIPT_DIR=$(cd $(dirname ${0}) >/dev/null 2>&1 && pwd)
declare -r ROOT_DIR=$(dirname ${SCRIPT_DIR})
declare -r UI_DIR="$ROOT_DIR/ui"

# This script is used to download built UI assets from the "influxdata/ui"
# repository. The built UI assets are attached to a release in "influxdata/ui",
# which is linked here.
# 
# The master branch of "influxdata/influxdb" (this repository) downloads from the
# release tagged as "OSS-Master" in "influxdata/ui". That release is kept up-to-date
# with the most recent changes in "influxdata/ui". 
# 
# Feature branches of "influxdata/influxdb" (2.0, 2.1, etc) download from their
# respective releases in "influxdata/ui" (OSS-2.0, OSS-2.1, etc). Those releases
# are updated only when a bug fix needs included for the UI of that OSS release.

curl -L https://github.com/influxdata/ui/releases/download/OSS-v2.0.7/build.tar.gz --output build.tar.gz
tar -xzf build.tar.gz -C "$UI_DIR"
rm build.tar.gz
