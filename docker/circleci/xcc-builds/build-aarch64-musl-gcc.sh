#!/usr/bin/env bash
set -euo pipefail

declare -r SCRIPT_DIR=$(cd $(dirname ${0}) >/dev/null 2>&1 && pwd)
declare -r OUT_DIR=${SCRIPT_DIR}/out

declare -r BUILD_IMAGE=circleci/golang:1.15-node-browsers
declare -r MUSL_VERSION=1.1.24
declare -r MUSL_CROSS_MAKE_VERSION=0.9.9

docker run --rm -i -v ${OUT_DIR}:/out -w /tmp ${BUILD_IMAGE} bash <<EOF
  set -euo pipefail

  declare -r BUILD_TIME=\$(date -u '+%Y%m%d%H%M%S')

  # Install dependencies.
  sudo apt-get update && sudo apt-get install -y --no-install-recommends patch

  # Clone and build musl-cross-make's ARM64 target.
  git clone https://github.com/richfelker/musl-cross-make.git && \
    cd musl-cross-make && \
    git checkout v${MUSL_CROSS_MAKE_VERSION} && \
    make MUSL_VER=${MUSL_VERSION} TARGET=aarch64-unknown-linux-musl install && \
    mv output /tmp/musl-cross && \
    cd /tmp

  # Archive the build output.
  tar czf /out/musl-${MUSL_VERSION}-cross-aarch64-${MUSL_CROSS_MAKE_VERSION}-\${BUILD_TIME}.tar.gz musl-cross
EOF
