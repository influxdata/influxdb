#!/bin/bash

function die() {
  echo "$@" 1>&2
  exit 1
}

GOOS=${GOOS:-$(go env GOOS)}
GOARCH=${GOARCH:-$(go env GOARCH)}
GOARM=${GOARM:-$(go env GOARM)}

case "${GOOS}_${GOARCH}_${GOARM}" in
  linux_amd64_)  CC=musl-gcc ;;
  linux_arm64_)  CC=aarch64-unknown-linux-musl-gcc ;;
  linux_arm_6)   CC=armv6-unknown-linux-musleabihf-gcc ;;
  linux_arm_7)   CC=armv7-unknown-linux-musleabihf-gcc ;;
  darwin_amd64_) CC=x86_64-apple-darwin15-clang ;;
  *) die "No cross-compiler set for ${GOOS}_${GOARCH}_${GOARM}" ;;
esac

exec ${CC} "$@"
