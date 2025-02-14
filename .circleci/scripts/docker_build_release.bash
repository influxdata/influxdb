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
REPO_URL="https://github.com/influxdata/influxdb_pro"

# not sure why default agent does not work, maybe because it already has another key?
eval `ssh-agent -s`
/usr/bin/ssh-add ~/.ssh/id_rsa_ca8e26e8972e85a859556d40cb3790d0
ssh-add -L

export DOCKER_BUILDKIT=1

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
  --ssh default=$SSH_AUTH_SOCK \
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
  --label org.opencontainers.image.title="InfluxDB3 Pro" \
  --label org.opencontainers.image.description="InfluxDB3 Pro Image" \
  --label com.influxdata.image.commit-date="$COMMIT_TS" \
  --label com.influxdata.image.package="$PACKAGE" \
  --progress plain \
  --tag "$TAG" \
  .
