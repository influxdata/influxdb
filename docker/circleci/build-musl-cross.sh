#!/usr/bin/env bash
set -euo pipefail

declare -r MUSL_VERSION=1.1.24
declare -r MUSL_CROSS_MAKE_VERSION=v0.9.9
declare -r BUILD_DOCKER=circleci/golang:1.15-node-browsers

function main() {
  cat <<EOF | docker run --rm -i -v $(pwd):/out -w /tmp ${BUILD_DOCKER}
sudo apt-get update && sudo apt-get install -y --no-install-recommends patch

git clone https://github.com/richfelker/musl-cross-make.git
cd musl-cross-make
git checkout ${MUSL_CROSS_MAKE_VERSION}

# ARM64.
make MUSL_VER=${MUSL_VERSION} TARGET=aarch64-unknown-linux-musl install
mv output musl-cross
tar czf /out/musl-cross-${MUSL_VERSION}-aarch64.tar.gz musl-cross
rm -rf musl-cross

# ARMv6. The +fp suffix is required for hard-float support.
make MUSL_VER=${MUSL_VERSION} TARGET=armv6-unknown-linux-musleabihf GCC_CONFIG='--with-arch=armv6+fp' install
mv output musl-cross
tar czf /out/musl-cross-${MUSL_VERSION}-armv6.tar.gz musl-cross
rm -rf musl-cross

# ARMv7. The -a subset is required for CGO compatibility. The +fp suffix is required for hard-float support.
make MUSL_VER=${MUSL_VERSION} TARGET=armv7-unknown-linux-musleabihf GCC_CONFIG='--with-arch=armv7-a+fp' install
mv output musl-cross
tar czf /out/musl-cross-${MUSL_VERSION}-armv7.tar.gz musl-cross
rm -rf musl-cross
EOF
}

main
