#!/bin/bash

function printHelp() {
  >&2 echo "USAGE: $0 -i PATH_TO_SOURCE_TARBALL -o OUTDIR

Runs unit tests for influxdb.

If the environment variable GO_NEXT is not empty, tests run with the 'next' version of Go.
"
}

if [ $# -eq 0 ]; then
  printHelp
  exit 1
fi

SRCDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SRCDIR/../_go_versions.sh"

OUTDIR=""
TARBALL=""

while getopts hi:o: arg; do
  case "$arg" in
    h) printHelp; exit 1;;
    i) TARBALL="$OPTARG";;
    o) OUTDIR="$OPTARG";;
  esac
done

if [ -z "$TARBALL" ] || [ -z "$OUTDIR" ]; then
  printHelp
  exit 1
fi

if [ -z "$GO_NEXT" ]; then
  DOCKER_TAG=latest
  GO_VERSION="$GO_CURRENT_VERSION"
else
  DOCKER_TAG=next
  GO_VERSION="$GO_NEXT_VERSION"
fi
docker build --build-arg "GO_VERSION=$GO_VERSION" -t influxdata/influxdb/releng/unit-tests:"$DOCKER_TAG" "$SRCDIR"

docker run --rm \
   --mount type=bind,source="$OUTDIR",destination=/out \
   --mount type=bind,source="$TARBALL",destination=/influxdb-src.tar.gz,ro=1 \
  influxdata/influxdb/releng/unit-tests:"$DOCKER_TAG"
