#!/bin/bash -e

function printHelp() {
  >&2 echo "USAGE: $0 -i PATH_TO_SOURCE_TARBALL -o OUTDIR

Emits an archive of influxdb binaries based on the current environment's GOOS and GOARCH.
"
}

if [ $# -eq 0 ]; then
  printHelp
  exit 1
fi

if [ -z "$GOOS" ] || [ -z "$GOARCH" ]; then
  >&2 echo 'The environment variables $GOOS and $GOARCH must both be set.'
  exit 1
fi

SRCDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SRCDIR/../_go_versions.sh"

OUTDIR=""
TARBALL=""
RACE_FLAG=""
STATIC_FLAG=""

while getopts hi:o:rs arg; do
  case "$arg" in
    h) printHelp; exit 1;;
    i) TARBALL="$OPTARG";;
    o) OUTDIR="$OPTARG";;
    r) RACE_FLAG="-r";;
    s) STATIC_FLAG="-s";;
  esac
done

if [ -z "$OUTDIR" ] || [ -z "$TARBALL" ]; then
  printHelp
  exit 1
fi

DOCKER_TAG=latest
GO_VERSION="$GO_CURRENT_VERSION"
docker build --build-arg "GO_VERSION=$GO_VERSION" -t influxdata/influxdb/releng/raw-binaries:"$DOCKER_TAG" "$SRCDIR"

mkdir -p "$OUTDIR"

docker run --rm \
   --mount type=bind,source="${OUTDIR}",destination=/out \
   --mount type=bind,source="${TARBALL}",destination=/influxdb-src.tar.gz,ro=1 \
   -e GOOS -e GOARCH \
  influxdata/influxdb/releng/raw-binaries:"$DOCKER_TAG" $RACE_FLAG $STATIC_FLAG
