#!/bin/bash

function printHelp() {
  >&2 echo "\
USAGE: $0 \\
  -s PATH_TO_SOURCE_TARBALL \\
  -b PATH_TO_BINARIES_TARBALL \\
  -O OS \\
  -A ARCH \\
  -o OUTDIR

Creates the given package type, using the given binaries in the tarball and
configuration files in the source tarball.
"
}

if [ $# -eq 0 ]; then
  printHelp
  exit 1
fi

SRCDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SRCDIR/../_go_versions.sh"

SRC_TARBALL=""
BIN_TARBALL=""
OS=""
ARCH=""
OUTDIR=""

while getopts hs:b:O:A:o: arg; do
  case "$arg" in
    h) printHelp; exit 1;;
    s) SRC_TARBALL="$OPTARG";;
    b) BIN_TARBALL="$OPTARG";;
    O) OS="$OPTARG";;
    A) ARCH="$OPTARG";;
    o) OUTDIR="$OPTARG";;
  esac
done

if [ -z "$OUTDIR" ] || [ -z "$SRC_TARBALL" ] || [ -z "$BIN_TARBALL" ] || [ -z "$OS" ] || [ -z "$ARCH" ]; then
  printHelp
  exit 1
fi

# Always build the latest version of the image.
docker build -t influxdata/influxdb/releng/packages:latest "$SRCDIR"

mkdir -p "$OUTDIR"

STATIC=""
case "$(basename "$SRC_TARBALL")" in
   *static*) STATIC="-s";;
esac

docker run --rm \
   --mount type=bind,source="${OUTDIR}",destination=/out \
   --mount type=bind,source="${SRC_TARBALL}",destination=/influxdb-src.tar.gz \
   --mount type=bind,source="${BIN_TARBALL}",destination=/influxdb-bin.tar.gz \
  influxdata/influxdb/releng/packages:latest -O "$OS" -A "$ARCH" $STATIC
