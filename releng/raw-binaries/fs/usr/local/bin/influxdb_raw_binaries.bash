#!/bin/bash

function printHelp() {
  >&2 echo "USAGE: $0 [-r]

Untars the influxdb source tarball mounted at /influxdb-src.tar.gz,
then emits a tarball of influxdb binaries to /out,
which must be a mounted volume if you want to access the file.

Relies upon environment variables GOOS and GOARCH to determine what to build.
Respects CGO_ENABLED.

To build with race detection enabled, pass the -r flag.
"
}

RACE_FLAG=""

while getopts hr arg; do
  case "$arg" in
    h) printHelp; exit 1;;
    r) RACE_FLAG="-race";;
  esac
done


if [ -z "$GOOS" ] || [ -z "$GOARCH" ]; then
  >&2 echo 'The environment variables $GOOS and $GOARCH must both be set.'
  exit 1
fi


# Extract tarball into GOPATH.
tar xz -C "$GOPATH" -f /influxdb-src.tar.gz

SHA=$(jq -r .sha < "$GOPATH/src/github.com/influxdata/influxdb/.metadata.json")


SUFFIX=
if [ "$CGO_ENABLED" == "0" ]; then
  # Only add the static suffix to the filename when explicitly requested.
  SUFFIX=_static
elif [ -n "$RACE_FLAG" ]; then
  # -race depends on cgo, so this option is exclusive from CGO_ENABLED.
  SUFFIX=_race
fi

TARBALL_NAME="influxdb_bin_${GOOS}_${GOARCH}${SUFFIX}-${SHA}.tar.gz"

# note: according to https://github.com/golang/go/wiki/GoArm
# we want to support armel using GOARM=5
# and we want to support armhf using GOARM=6
# no GOARM setting is necessary for arm64
if [ $GOARCH == "armel" ]; then
  GOARCH=arm
  GOARM=5
fi

if [ $GOARCH == "armhf" ]; then
  GOARCH=arm
  GOARM=6
fi



OUTDIR=$(mktemp -d)
for cmd in \
  influxdb/cmd/influxd \
  influxdb/cmd/influx_stress \
  influxdb/cmd/influx \
  influxdb/cmd/influx_inspect \
  influxdb/cmd/influx_tsm \
  ; do
    # Build all the binaries into $OUTDIR.
    # Windows binaries will get the .exe suffix as expected.
    (cd "$OUTDIR" && go build $RACE_FLAG -i "github.com/influxdata/$cmd")
done


(cd "$OUTDIR" && tar czf "/out/$TARBALL_NAME" ./*)
(cd /out && md5sum "$TARBALL_NAME" > "$TARBALL_NAME.md5")
(cd /out && sha256sum "$TARBALL_NAME" > "$TARBALL_NAME.sha256")
