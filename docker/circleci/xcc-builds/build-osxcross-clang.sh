#!/usr/bin/env bash
set -euo pipefail

declare -r SCRIPT_DIR=$(cd $(dirname ${0}) >/dev/null 2>&1 && pwd)
declare -r OUT_DIR=${SCRIPT_DIR}/out

declare -r BUILD_IMAGE=circleci/golang:1.15-node-browsers
declare -r OSXCROSS_VERSION=c2ad5e859d12a295c3f686a15bd7181a165bfa82

docker run --rm -i -v ${OUT_DIR}:/out -w /tmp ${BUILD_IMAGE} bash <<EOF
  set -euo pipefail

  declare -r BUILD_TIME=\$(date -u '+%Y%m%d%H%M%S')

  # Install dependencies.
  sudo apt-get update && sudo apt-get install -y --no-install-recommends \
    clang \
    cmake \
    libssl-dev \
    libxml2-dev \
    llvm-dev \
    lzma-dev \
    patch \
    zlib1g-dev

  # Clone and build osxcross.
  sudo git clone https://github.com/tpoechtrager/osxcross.git /usr/local/osxcross && \
    cd /usr/local/osxcross && \
    sudo git checkout ${OSXCROSS_VERSION} && \
    sudo curl -L -o ./tarballs/MacOSX10.11.sdk.tar.xz https://macos-sdks.s3.amazonaws.com/MacOSX10.11.sdk.tar.xz && \
    sudo UNATTENDED=1 PORTABLE=true OCDEBUG=1 ./build.sh && \
    sudo rm -rf .git build tarballs && \
    cd /tmp

  # Archive the build output.
  cd /usr/local && tar czf /out/osxcross-${OSXCROSS_VERSION}-\${BUILD_TIME}.tar.gz osxcross && cd /tmp
EOF
