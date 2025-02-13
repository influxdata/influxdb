#!/usr/bin/env bash

set -euo pipefail

readonly PACKAGE="$1"
readonly FEATURES="$2"
readonly TAG="$3"
readonly PBS_DATE="$4"
readonly PBS_VERSION="$5"
readonly ARCH="${6:-amd64}"  # Default to amd64 if not specified
readonly PROFILE="${7:-release}"       # Default to release if not specified

RUST_VERSION="$(sed -E -ne 's/channel = "(.*)"/\1/p' rust-toolchain.toml)"
COMMIT_SHA="$(git rev-parse HEAD)"
COMMIT_TS="$(env TZ=UTC0 git show --quiet --date='format-local:%Y-%m-%dT%H:%M:%SZ' --format="%cd" HEAD)"
NOW="$(date --utc --iso-8601=seconds)"
REPO_URL="https://github.com/influxdata/influxdb"

# Convert arch to platform
PLATFORM="linux/${ARCH}"

# Convert arch to python-build-standalone target
PBS_TARGET=
case "$PLATFORM" in
  linux/amd64)
    PBS_TARGET="x86_64-unknown-linux-gnu"
    ;;
  linux/arm64)
    PBS_TARGET="aarch64-unknown-linux-gnu"
    ;;
  *)
    echo "Unknown python-build-standalone platform: '$PLATFORM'"
    exit 1
esac

exec docker buildx build \
  --build-arg CARGO_INCREMENTAL="no" \
  --build-arg CARGO_NET_GIT_FETCH_WITH_CLI="true" \
  --build-arg FEATURES="$FEATURES" \
  --build-arg RUST_VERSION="$RUST_VERSION" \
  --build-arg PACKAGE="$PACKAGE" \
  --build-arg PROFILE="$PROFILE" \
  --build-arg PBS_TARGET="$PBS_TARGET" \
  --build-arg PBS_DATE="$PBS_DATE" \
  --build-arg PBS_VERSION="$PBS_VERSION" \
  --platform "$PLATFORM" \
  --label org.opencontainers.image.created="$NOW" \
  --label org.opencontainers.image.url="$REPO_URL" \
  --label org.opencontainers.image.revision="$COMMIT_SHA" \
  --label org.opencontainers.image.vendor="InfluxData Inc." \
  --label org.opencontainers.image.title="InfluxDB3 Edge" \
  --label org.opencontainers.image.description="InfluxDB3 Edge Image" \
  --label com.influxdata.image.commit-date="$COMMIT_TS" \
  --label com.influxdata.image.package="$PACKAGE" \
  --progress plain \
  --tag "$TAG" \
  .
