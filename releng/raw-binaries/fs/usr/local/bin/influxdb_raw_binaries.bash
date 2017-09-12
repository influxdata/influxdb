#!/bin/bash

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
fi

TARBALL_NAME="influxdb_bin_${GOOS}_${GOARCH}${SUFFIX}-${SHA}.tar.gz"

# note: according to https://github.com/golang/go/wiki/GoArm
# we want to support armel using GOARM=5
# and we want to support armhf using GOARM=6
# no GOARM setting is necessary for arm64
if [ $GOARCH == "armel"]; then
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
    go build -i -o "$OUTDIR/$(basename $cmd)" "github.com/influxdata/$cmd"
done


(cd "$OUTDIR" && tar czf "/out/$TARBALL_NAME" ./*)
(cd /out && md5sum "$TARBALL_NAME" > "$TARBALL_NAME.md5")
(cd /out && sha256sum "$TARBALL_NAME" > "$TARBALL_NAME.sha256")
