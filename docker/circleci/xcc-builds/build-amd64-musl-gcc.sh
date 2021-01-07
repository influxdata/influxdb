#!/usr/bin/env bash
set -euo pipefail

declare -r SCRIPT_DIR=$(cd $(dirname ${0}) >/dev/null 2>&1 && pwd)
declare -r OUT_DIR=${SCRIPT_DIR}/out

declare -r BUILD_IMAGE=circleci/golang:1.15-node-browsers
declare -r MUSL_VERSION=1.1.24

docker run --rm -i -v ${OUT_DIR}:/out -w /tmp ${BUILD_IMAGE} bash <<EOF
  set -euo pipefail

  declare -r BUILD_TIME=\$(date -u '+%Y%m%d%H%M%S')

  # Install dependencies.
  sudo apt-get update && sudo apt-get install -y --no-install-recommends patch

  # Build MUSL from source.
  curl https://musl.libc.org/releases/musl-${MUSL_VERSION}.tar.gz -O && \
    tar xzf musl-${MUSL_VERSION}.tar.gz && \
    cd musl-${MUSL_VERSION} &&
    ./configure &&
    make && \
    sudo make install && \
    cd /tmp

  # Archive the build output.
  cd /usr/local && tar czf /out/musl-${MUSL_VERSION}-\${BUILD_TIME}.tar.gz musl && cd /tmp
EOF
