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

# Download the SHA256 checksum attached to the release. To verify the integrity
# of the download, this checksum will be used to check the download tar file
# containing the built UI assets.
curl -Ls https://github.com/influxdata/ui/releases/download/OSS-Master/sha256.txt --output sha256.txt

# Download the tar file containing the built UI assets.
curl -L https://github.com/influxdata/ui/releases/download/OSS-Master/build.tar.gz --output build.tar.gz

# Verify the checksums match; exit if they don't.
if [ $(uname) = Darwin ]; then
    # MacOS doesn't come with sha256sum natively.
    # It's available via `brew install coreutils` but requires some extra PATH-setting,
    # so this is probably easier for end users.
    echo "$(cat sha256.txt)" | shasum -a 256 --check -- \
        || { echo "Checksums did not match for downloaded UI assets!"; exit 1; }
else
    echo "$(cat sha256.txt)" | sha256sum --check -- \
        || { echo "Checksums did not match for downloaded UI assets!"; exit 1; }
fi

# Extract the assets and clean up.
mkdir -p "$STATIC_DIR/data"
tar -xzf build.tar.gz -C "$STATIC_DIR/data"
rm sha256.txt
rm build.tar.gz
