#!/bin/bash

function printHelp() {
  >&2 echo \
"USAGE: $0 [-p INFLUXDB_GIT_DIR]
            -s INFLUXDB_SHA -b INFLUXDB_BRANCH -v INFLUXDB_VERSION -o OUTDIR

Emits a tarball of influxdb source code and dependencies to OUTDIR.

If using -p flag, directory containing influxdb source code will be used as source of truth.
This is helpful if you have local commits that have not been pushed.
"
}

if [ $# -eq 0 ]; then
  printHelp
  exit 1
fi

SRCDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SRCDIR/../_go_versions.sh"

SHA=""
BRANCH=""
VERSION=""
OUTDIR=""

# These variables may expand to command arguments. Don't double quote them when used later.
INFLUXDB_GIT_MOUNT=""


while getopts hs:b:v:o:p: arg; do
  case "$arg" in
    h) printHelp; exit 1;;
    s) SHA="$OPTARG";;
    b) BRANCH="$OPTARG";;
    v) VERSION="$OPTARG";;
    o) OUTDIR="$OPTARG";;
    p) INFLUXDB_GIT_MOUNT="--mount type=bind,src=$OPTARG,dst=/influxdb-git,ro=1";;
  esac
done

if [ -z "$OUTDIR" ]; then
  # Not bothering to check the other variables since they're checked in the inner docker script.
  printHelp
  exit 1
fi

# Only build with GO_CURRENT_VERSION. No need to build source tarball with next version of Go.
docker build --build-arg "GO_VERSION=$GO_CURRENT_VERSION" -t influxdata/influxdb/releng/source-tarball:latest "$SRCDIR"

mkdir -p "$OUTDIR"

docker run --rm \
  $INFLUXDB_GIT_MOUNT \
  --mount "type=bind,src=${OUTDIR},dst=/out" \
  influxdata/influxdb/releng/source-tarball:latest \
  -s "$SHA" \
  -b "$BRANCH" \
  -v "$VERSION"
