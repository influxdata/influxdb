#!/bin/bash

function die() {
  echo "$@" 1>&2
  exit 1
}

GOOS=${GOOS:-$(go env GOOS)}
GOARCH=${GOARCH:-$(go env GOARCH)}

case "${GOOS}_${GOARCH}" in
  linux_amd64) CC=clang ;;
  linux_arm64) CC=aarch64-linux-gnu-gcc ;;
  linux_arm)   CC=arm-linux-gnueabihf-gcc ;;
  darwin_amd64) CC=o64-clang ;;
  *) die "No cross-compiler set for ${GOOS}_${GOARCH}" ;;
esac

exec ${CC} "$@"
