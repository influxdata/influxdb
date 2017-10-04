#!/bin/bash

function printHelp() {
  >&2 echo "\
USAGE: $0 -p PKG_FILE [-D | -R]

Tests installing and then uninstalling the provided package files.
At least one of -D or -R must be provided to indicate Debian or RPM packages.
"
}

if [ $# -eq 0 ]; then
  printHelp
  exit 1
fi

PKG=""
IS_DEB=""
IS_RPM=""

while getopts hp:DR arg; do
  case "$arg" in
    h) printHelp; exit 1;;
    p) PKG="$OPTARG";;
    D) IS_DEB="1";;
    R) IS_RPM="1";;
  esac
done

if [ -z "$PKG" ] ; then
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
  local typeArg="$3"

  docker run --rm \
    --mount "type=bind,src=$SRCDIR/_install_uninstall.bash,dst=/usr/bin/install_uninstall.bash,ro=1" \
    --mount "type=bind,src=${pkgSrc},dst=${pkgDst},ro=1" \
    "$BASE_IMAGE" install_uninstall.bash "$typeArg"
}

if [ -n "$IS_DEB" ]; then
  # Latest is the most recent LTS, and Rolling is the most recent release.
  for BASE_IMAGE in ubuntu:latest ubuntu:rolling ; do
    if [ -n "$PKG" ]; then
      dockerTest "$PKG" /data.deb -D
    fi
  done
fi

if [ -n "$IS_RPM" ]; then
  # Latest is the most recent LTS, and Rolling is the most recent release.
  for BASE_IMAGE in centos:6 centos:7 ; do
    if [ -n "$PKG" ]; then
      dockerTest "$PKG" /data.rpm -R
    fi
  done
fi
