#!/usr/bin/env bash

set -euo pipefail

readonly PACKAGE="$1"
readonly FEATURES="$2"
readonly TAG="$3"

RUST_VERSION="$(sed -E -ne 's/channel = "(.*)"/\1/p' rust-toolchain.toml)"
COMMIT_SHA="$(git rev-parse HEAD)"
COMMIT_TS="$(env TZ=UTC0 git show --quiet --date='format-local:%Y-%m-%dT%H:%M:%SZ' --format="%cd" HEAD)"
NOW="$(date --utc --iso-8601=seconds)"
REPO_URL="https://github.com/influxdata/influxdb_iox"

exec docker buildx build \
  --build-arg CARGO_INCREMENTAL="no" \
  --build-arg CARGO_NET_GIT_FETCH_WITH_CLI="true" \
  --build-arg FEATURES="$FEATURES" \
  --build-arg RUST_VERSION="$RUST_VERSION" \
  --build-arg PACKAGE="$PACKAGE" \
  --label org.opencontainers.image.created="$NOW" \
  --label org.opencontainers.image.url="$REPO_URL" \
  --label org.opencontainers.image.revision="$COMMIT_SHA" \
  --label org.opencontainers.image.vendor="InfluxData Inc." \
  --label org.opencontainers.image.title="InfluxDB IOx, '$PACKAGE'" \
  --label org.opencontainers.image.description="InfluxDB IOx production image for package '$PACKAGE'" \
  --label com.influxdata.image.commit-date="$COMMIT_TS" \
  --label com.influxdata.image.package="$PACKAGE" \
  --progress plain \
  --tag "$TAG" \
  .
