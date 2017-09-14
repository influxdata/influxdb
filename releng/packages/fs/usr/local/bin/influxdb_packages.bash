#!/bin/bash

set -e

function printHelp() {
  >&2 echo "\
USAGE: $0 -O OS -A ARCH

Creates packages for the given OS/ARCH, using the influxdb source tarball mounted at
/influxdb-src.tar.gz and the binaries tarball mounted at /influxdb-bin.tar.gz .
"
}

if [ $# -eq 0 ]; then
  printHelp
  exit 1
fi

OS=""
ARCH=""

while getopts hO:A: arg; do
  case "$arg" in
    h) printHelp; exit 1;;
    O) OS="$OPTARG";;
    A) ARCH="$OPTARG";;
  esac
done

if [ -z "$OS" ] || [ -z "$ARCH" ]; then
  printHelp
  exit 1
fi

mkdir -p /go
tar x -C /go -zf /influxdb-src.tar.gz
ln -s /go/src/github.com/influxdata/influxdb /isrc # Shorthand for influxdb source.
SHA=$(jq -r .sha < "/isrc/.metadata.json")
VERSION=$(jq -r .version < "/isrc/.metadata.json")

# Extract the respective binaries to dedicated folders.
mkdir -p /ibin
(cd /ibin && tar xzf /influxdb-bin.tar.gz)

if [ "$OS" == "linux" ]; then
  #############################
  ####### Data packages #######
  #############################

  # Create layout for packaging in /pkg.
  mkdir -p /pkg/usr/bin \
           /pkg/var/log/influxdb \
           /pkg/var/lib/influxdb \
           /pkg/usr/lib/influxdb/scripts \
           /pkg/etc/influxdb \
           /pkg/etc/logrotate.d
  chmod -R 0755 /pkg

  # Copy service scripts.
  cp /isrc/scripts/init.sh /pkg/usr/lib/influxdb/scripts/init.sh
  chmod 0644 /pkg/usr/lib/influxdb/scripts/init.sh
  cp /isrc/scripts/influxdb.service /pkg/usr/lib/influxdb/scripts/influxdb.service
  chmod 0644 /pkg/usr/lib/influxdb/scripts/influxdb.service

  # Copy logrotate script.
  cp /isrc/scripts/logrotate /pkg/etc/logrotate.d/influxdb
  chmod 0644 /pkg/etc/logrotate.d/influxdb

  # Copy sample config.
  cp /isrc/etc/config.sample.toml /pkg/etc/influxdb/influxdb.conf

  # Copy data binaries.
  cp /ibin/* /pkg/usr/bin/

  # Make tarball of files in packaging.
  (cd /pkg && tar czf "/out/influxdb-pkg-${OS}-${ARCH}-${SHA}.tar.gz" ./*)

  # Call fpm to build .deb and .rpm packages.
  for typeargs in "-t deb" "-t rpm --depends coreutils"; do
    fpm \
      -s dir \
      $typeargs \
      --log error \
      --vendor InfluxData \
      --url "https://influxdata.com" \
      --after-install /isrc/scripts/post-install.sh \
      --before-install /isrc/scripts/pre-install.sh \
      --after-remove /isrc/scripts/post-uninstall.sh \
      --license Proprietary \
      --maintainer "support@influxdb.com" \
      --directories /var/log/influxdb \
      --directories /var/lib/influxdb \
      --description 'Distributed time-series database.' \
      --config-files /etc/influxdb/influxdb.conf \
      --config-files /etc/logrotate.d/influxdb \
      --name "influxdb" \
      --architecture "$ARCH" \
      --version "$VERSION" \
      --iteration 1 \
      -C /pkg \
      -p /out
  done

  #############################
  ######### Checksums #########
  #############################
  (cd /out && for f in *.deb *.rpm; do
    md5sum "$f" > "$f.md5"
    sha256sum "$f" > "$f.sha256"
  done)
elif [ "$OS" == "windows" ]; then
  # Windows gets the binaries and nothing else.
  # TODO: should Windows get the sample config files?
  (cd /ibin && zip -9 -r "/out/influxdb_${VERSION}.zip" ./*)
  (cd /out && for f in *.zip; do
    md5sum "$f" > "$f.md5"
    sha256sum "$f" > "$f.sha256"
  done)
else
  >&2 echo "Unrecognized OS: $OS"
  exit 1
fi
