#!/bin/bash

set -e

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
STATIC=""

while getopts w:hrs arg; do
  case "$arg" in
    h) printHelp; exit 1;;
    r) RACE_FLAG="-race";;
    s) STATIC=1;;
  esac
done

if [ -z "$GOOS" ] || [ -z "$GOARCH" ]; then
  >&2 echo 'The environment variables $GOOS and $GOARCH must both be set.'
  exit 1
fi

# Control the compiler for go. Rust linker is set in $HOME/.cargo/config
if [[ "$GOOS" == darwin ]] ; then
  export CC=x86_64-apple-darwin15-clang
elif [[ "$GOOS" == windows ]] ; then
  export CC=x86_64-w64-mingw32-gcc
elif [[ "$GOARCH" == arm64 ]] ;then
  export CC=aarch64-unknown-linux-musl-gcc
fi

WORKSPACE=/influxdata

mkdir -p ${WORKSPACE}

echo "Extracting influxdb tarball"
# Extract tarball into WORKSPACE.

tar -vxz -C ${WORKSPACE} -f /influxdb-src.tar.gz

SHA=$(jq -r .sha < "${WORKSPACE}/influxdb/.metadata.json")

OUTDIR=$(mktemp -d)
(
	cd ${WORKSPACE}/influxdb

	BINARY_PACKAGES="
		github.com/influxdata/influxdb/cmd/influxd
		github.com/influxdata/influxdb/cmd/influx
		github.com/influxdata/influxdb/cmd/influx_inspect"

	for cmd in $BINARY_PACKAGES; do
		export CGO_ENABLED=1
		echo "env for go build: GOOS=$GOOS GOARCH=$GOARCH CGO_ENABLED=$CGO_ENABLED"
		# Note that we only do static builds for arm, to be consistent with influxdb 2.x
		if [[ "$GOARCH" == arm64 ]] ; then
			echo go build -i -o "$OUTDIR/$(basename $cmd)" -tags "netgo osusergo static_build noasm" $cmd
			go build -i -o "$OUTDIR/$(basename $cmd)" -tags "netgo osusergo static_build noasm" $cmd
		elif [[ -n "$STATIC" ]]; then
			echo go build -i -o "$OUTDIR/$(basename $cmd)" -tags "netgo osusergo static_build" $cmd
			go build -i -o "$OUTDIR/$(basename $cmd)" -tags "netgo osusergo static_build" $cmd
		elif [[ "$GOOS" == windows ]] ; then
			echo go build $RACE_FLAG -buildmode=exe -i -o "$OUTDIR/$(basename $cmd).exe" $cmd
			go build $RACE_FLAG -buildmode=exe -i -o "$OUTDIR/$(basename $cmd).exe" $cmd
		else
			echo go build $RACE_FLAG -i -o "$OUTDIR/$(basename $cmd)" $cmd
			go build $RACE_FLAG -i -o "$OUTDIR/$(basename $cmd)" $cmd
		fi
	done
)

SUFFIX=
if [[ -n "$STATIC" ]]; then
  # Only add the static suffix to the filename when explicitly requested.
  SUFFIX=_static
elif [ -n "$RACE_FLAG" ]; then
  # -race depends on cgo, so this option is exclusive from CGO_ENABLED.
  SUFFIX=_race
fi

TARBALL_NAME="influxdb_bin_${GOOS}_${GOARCH}${SUFFIX}-${SHA}.tar.gz"
TARBALL_PATH="/out/${TARBALL_NAME}"
echo tar -C ${OUTDIR} -cvzf ${TARBALL_PATH} .
tar -C ${OUTDIR} -cvzf ${TARBALL_PATH} .
(cd /out && md5sum "$TARBALL_NAME" > "$TARBALL_NAME.md5")
(cd /out && sha256sum "$TARBALL_NAME" > "$TARBALL_NAME.sha256")
