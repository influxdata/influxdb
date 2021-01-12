#!/usr/bin/env bash
set -euo pipefail

declare -r SCRIPT_DIR=$(cd $(dirname ${0}) >/dev/null 2>&1 && pwd)
declare -r OUT_DIR=${SCRIPT_DIR}/out

declare -r BUILD_IMAGE=ubuntu:20.04
declare -r MUSL_VERSION=1.1.24

docker run --rm -i -v ${OUT_DIR}:/out -w /tmp ${BUILD_IMAGE} bash <<EOF
  set -euo pipefail

  declare -r BUILD_TIME=\$(date -u '+%Y%m%d%H%M%S')

  # Install dependencies.
  apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    curl \
    patch

  # Build MUSL from source.
  curl https://musl.libc.org/releases/musl-${MUSL_VERSION}.tar.gz -O && \
    tar xzf musl-${MUSL_VERSION}.tar.gz && \
    cd musl-${MUSL_VERSION} &&
    ./configure &&
    make && \
    make install && \
    cd /tmp

  # Archive the build output.
  cd /usr/local && tar czf /out/musl-${MUSL_VERSION}-\${BUILD_TIME}.tar.gz musl && cd /tmp
EOF
