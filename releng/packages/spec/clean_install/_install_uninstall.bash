#!/bin/bash

##########################################################
# This script is intended to be run from inside Docker.  #
#                                                        #
# You generally can't start services from inside Docker, #
# so we just assert that binaries are on $PATH.          #
##########################################################

function printHelp() {
  >&2 echo "\
USAGE: $0 [-d | -m] [-D | -R]

Tests installing and then uninstalling the provided package files.
Exactly one of -d or -m must be provided to indicate data or meta,
and exactly one of -D or -R must be provided to indicate Debian or RPM packages.
"
}

DATA_BINS=( influx influxd influx_stress influx_inspect influx_tsm )
META_BINS=( influxd-meta influxd-ctl )

function testInstalled() {
  if ! command -v "$1" >/dev/null 2>&1 ; then
    >&2 echo "$1 not on \$PATH after install"
    exit 1
  fi
}

function testUninstalled() {
  if command -v "$1" >/dev/null 2>&1 ; then
    >&2 echo "$1 still on \$PATH after install"
    exit 1
  fi
}

function testData() {
  if [ "$TYPE" == "deb" ]; then
    dpkg -i /data.deb
  elif [ "$TYPE" == "rpm" ]; then
    yum localinstall -y /data.rpm
  else
    >&2 echo "testMeta: invalid type $TYPE"
    exit 2
  fi

  for x in "${DATA_BINS[@]}"; do
    testInstalled "$x"
  done

  if [ "$TYPE" == "deb" ]; then
    dpkg -r influxdb-data
  elif [ "$TYPE" == "rpm" ]; then
    yum remove -y influxdb-data
  fi

  for x in "${DATA_BINS[@]}"; do
    testUninstalled "$x"
  done

  true # So we don't return 1 if `which` didn't find the executable after uninstall.
}

function testMeta() {
  if [ "$TYPE" == "deb" ]; then
    dpkg -i /meta.deb
  elif [ "$TYPE" == "rpm" ]; then
    yum localinstall -y /meta.rpm
  else
    >&2 echo "testMeta: invalid type $TYPE"
    exit 2
  fi

  for x in "${META_BINS[@]}"; do
    testInstalled "$x"
  done

  if [ "$TYPE" == "deb" ]; then
    dpkg -r influxdb-meta
  elif [ "$TYPE" == "rpm" ]; then
    yum remove -y influxdb-meta
  fi

  for x in "${META_BINS[@]}"; do
    testUninstalled "$x"
  done

  true # So we don't return 1 if `test{Uni,I}nstalled` didn't find the executable after uninstall.
}

PKG=""
TYPE=""

while getopts dmDR arg; do
  case "$arg" in
    d) PKG=data;;
    m) PKG=meta;;
    D) TYPE=deb;;
    R) TYPE=rpm;;
  esac
done

if [ "$TYPE" != "deb" ] && [ "$TYPE" != "rpm" ]; then
  printHelp
  exit 1
fi

if [ "$PKG" == "data" ]; then
  testData
elif [ "$PKG" == "meta" ]; then
  testMeta
else
  printHelp
  exit 1
fi
