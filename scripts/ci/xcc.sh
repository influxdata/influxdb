#!/bin/bash

function die() {
  echo "$@" 1>&2
  exit 1
}

GOOS=${GOOS:-$(go env GOOS)}
GOARCH=${GOARCH:-$(go env GOARCH)}

case "${GOOS}_${GOARCH}" in
  linux_amd64)   CC=musl-gcc ;;
  linux_arm64)   CC=aarch64-unknown-linux-musl-gcc ;;
  linux_s390x) CC=s390x-linux-gnu-gcc ;;
  darwin_amd64)  CC=x86_64-apple-darwin15-clang ;;
  windows_amd64) CC=x86_64-w64-mingw32-gcc ;;
  *) die "No cross-compiler set for ${GOOS}_${GOARCH}" ;;
esac

exec ${CC} "$@"
