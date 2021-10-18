#!/usr/bin/env bash

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

set -e

declare -r SCRIPT_DIR=$(cd $(dirname ${0}) >/dev/null 2>&1 && pwd)
declare -r ROOT_DIR=$(dirname ${SCRIPT_DIR})
declare -r STATIC_DIR="$ROOT_DIR/static"

# This must be updated depending on the actual sha256 checksum of the tar file
# attached to the release.
EXPECTED_UI_CHECKSUM=f90ada02d0e09082b36e4213f2a675089e95e01a9558bc356a0d880e2aaaccd1

# Download the tar file containing the built UI assets.
curl -L https://github.com/influxdata/ui/releases/download/OSS-2.1.0/build.tar.gz --output build.tar.gz

# Verify the checksums match; exit if they don't.
echo "${EXPECTED_UI_CHECKSUM} build.tar.gz" | sha256sum --check -- \
    || { echo "Checksums did not match for downloaded UI assets!"; exit 1; }

# Extract the assets and clean up.
mkdir -p "$STATIC_DIR/data"
tar -xzf build.tar.gz -C "$STATIC_DIR/data"
rm build.tar.gz
