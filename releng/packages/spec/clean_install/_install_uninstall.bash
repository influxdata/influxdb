#!/bin/bash

##########################################################
# This script is intended to be run from inside Docker.  #
#                                                        #
# You generally can't start services from inside Docker, #
# so we just assert that binaries are on $PATH.          #
##########################################################

function printHelp() {
  >&2 echo "\
USAGE: $0 [-D | -R]

Tests installing and then uninstalling the provided package files.
Exactly one of -D or -R must be provided to indicate Debian or RPM packages.
"
}

BINS=( influx influxd influx_stress influx_inspect influx_tsm )


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

function testInstall() {
  if [ "$TYPE" == "deb" ]; then
    dpkg -i /data.deb
  elif [ "$TYPE" == "rpm" ]; then
    yum localinstall -y /data.rpm
  else
    >&2 echo "testInstall: invalid type $TYPE"
    exit 2
  fi

  for x in "${BINS[@]}"; do
    testInstalled "$x"
  done

  if [ "$TYPE" == "deb" ]; then
    dpkg -r influxdb
  elif [ "$TYPE" == "rpm" ]; then
    yum remove -y influxdb
  fi

  for x in "${BINS[@]}"; do
    testUninstalled "$x"
  done

  true # So we don't return 1 if `which` didn't find the executable after uninstall.
}

PKG=""
TYPE=""

while getopts DR arg; do
  case "$arg" in
    D) TYPE=deb;;
    R) TYPE=rpm;;
  esac
done

if [ "$TYPE" != "deb" ] && [ "$TYPE" != "rpm" ]; then
  printHelp
  exit 1
fi

testInstall
