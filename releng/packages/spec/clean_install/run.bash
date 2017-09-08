#!/bin/bash

function printHelp() {
  >&2 echo "\
USAGE: $0 -d DATA_PKG_FILE -m META_PKG_FILE [-D | -R]

Tests installing and then uninstalling the provided package files.
At least one of -d or -m must be provided, and exactly one of
-D or -R must be provided to indicate Debian or RPM packages.
"
}

if [ $# -eq 0 ]; then
  printHelp
  exit 1
fi

DATA_PKG=""
META_PKG=""
IS_DEB=""
IS_RPM=""

while getopts hd:m:DR arg; do
  case "$arg" in
    h) printHelp; exit 1;;
    d) DATA_PKG="$OPTARG";;
    m) META_PKG="$OPTARG";;
    D) IS_DEB="1";;
    R) IS_RPM="1";;
  esac
done

if [ -z "$DATA_PKG" ] && [ -z "$META_PKG" ]; then
  printHelp
  exit 1
fi

if [ "${IS_DEB}${IS_RPM}" -ne "1" ]; then
  printHelp
  exit 1
fi

set -e

SRCDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

function dockerTest() {
  local pkgSrc="$1"
  local pkgDst="$2"
  local pkgArg="$3"
  local typeArg="$4"

  docker run --rm \
    --mount "type=bind,src=$SRCDIR/_install_uninstall.bash,dst=/usr/bin/install_uninstall.bash,ro=1" \
    --mount "type=bind,src=${pkgSrc},dst=${pkgDst},ro=1" \
    "$BASE_IMAGE" install_uninstall.bash "$pkgArg" "$typeArg"
}

if [ -n "$IS_DEB" ]; then
  # Latest is the most recent LTS, and Rolling is the most recent release.
  for BASE_IMAGE in ubuntu:latest ubuntu:rolling ; do
    if [ -n "$DATA_PKG" ]; then
      dockerTest "$DATA_PKG" /data.deb -d -D
    fi

    if [ -n "$META_PKG" ]; then
      dockerTest "$META_PKG" /meta.deb -m -D
    fi
  done
fi

if [ -n "$IS_RPM" ]; then
  # Latest is the most recent LTS, and Rolling is the most recent release.
  for BASE_IMAGE in centos:6 centos:7 ; do
    if [ -n "$DATA_PKG" ]; then
      dockerTest "$DATA_PKG" /data.rpm -d -R
    fi

    if [ -n "$META_PKG" ]; then
      dockerTest "$META_PKG" /meta.rpm -m -R
    fi
  done
fi
