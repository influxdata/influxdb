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
STATIC=""

while getopts hO:A:s arg; do
  case "$arg" in
    h) printHelp; exit 1;;
    O) OS="$OPTARG";;
    # For backwards compatibility, ensure the packages say i386 if using GOARCH=386.
    A) ARCH="$(echo "$OPTARG" | sed 's/386/i386/')";;
    s) STATIC="1";;
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
ARCHIVE_ROOT_NAME="influxdb-${VERSION}-1"
PKG_ROOT="/pkg/$ARCHIVE_ROOT_NAME"

# Extract the respective binaries to dedicated folders.
mkdir -p /ibin
(cd /ibin && tar xzf /influxdb-bin.tar.gz)

if [ "$OS" == "linux" ] && [ "$STATIC" == "1" ]; then
  # Static linux packages get only the binaries and the conf file in the root directory,
  # plus the man pages in the full path.
  rm -rf "$PKG_ROOT"
  mkdir -p "$PKG_ROOT"

  cp /ibin/* "$PKG_ROOT/"
  cp /isrc/etc/config.sample.toml "$PKG_ROOT/influxdb.conf"

  mkdir -p "$PKG_ROOT/usr/share/man/man1"
  cp /isrc/man/*.1.gz "$PKG_ROOT/usr/share/man/man1"

  # Creating tarball from /pkg, NOT from $PKG_ROOT, so that influxdb-$VERSION-1 directory is present in archive.
  (cd /pkg && tar czf "/out/influxdb-${VERSION}-static_${OS}_${ARCH}.tar.gz" ./*)

  (cd /out && for f in *.tar.gz; do
    md5sum "$f" > "$f.md5"
    sha256sum "$f" > "$f.sha256"
  done)
elif [ "$OS" == "linux" ] || [ "$OS" == "darwin" ]; then
  #############################
  ####### Data packages #######
  #############################

  # Create layout for packaging under $PKG_ROOT.
  rm -rf "$PKG_ROOT"
  mkdir -p "$PKG_ROOT/usr/bin" \
           "$PKG_ROOT/var/log/influxdb" \
           "$PKG_ROOT/var/lib/influxdb" \
           "$PKG_ROOT/usr/lib/influxdb/scripts" \
           "$PKG_ROOT/usr/share/man/man1" \
           "$PKG_ROOT/etc/influxdb" \
           "$PKG_ROOT/etc/logrotate.d"
  chmod -R 0755 /pkg

  # Copy service scripts.
  cp /isrc/scripts/init.sh "$PKG_ROOT/usr/lib/influxdb/scripts/init.sh"
  chmod 0644 "$PKG_ROOT/usr/lib/influxdb/scripts/init.sh"
  cp /isrc/scripts/influxdb.service "$PKG_ROOT/usr/lib/influxdb/scripts/influxdb.service"
  chmod 0644 "$PKG_ROOT/usr/lib/influxdb/scripts/influxdb.service"

  # Copy logrotate script.
  cp /isrc/scripts/logrotate "$PKG_ROOT/etc/logrotate.d/influxdb"
  chmod 0644 "$PKG_ROOT/etc/logrotate.d/influxdb"

  # Copy sample config.
  cp /isrc/etc/config.sample.toml "$PKG_ROOT/etc/influxdb/influxdb.conf"

  # Copy data binaries.
  cp /ibin/* "$PKG_ROOT/usr/bin/"

  # Copy man pages.
  cp /isrc/man/*.1.gz "$PKG_ROOT/usr/share/man/man1"

  # Make tarball of files in packaging.
  BIN_GZ_NAME="/out/influxdb-${VERSION}_${OS}_${ARCH}.tar.gz"
  if [ "$STATIC" == "1" ]; then
    BIN_GZ_NAME="/out/influxdb-${VERSION}-static_${OS}_${ARCH}.tar.gz"
  fi

  # Creating tarball from /pkg, NOT from $PKG_ROOT, so that influxdb-$VERSION-1 directory is present in archive.
  (cd /pkg && tar czf $BIN_GZ_NAME ./*)

  if [ "$OS" == "linux" ] ; then
    # Call fpm to build .deb and .rpm packages.
    for typeargs in "-t deb" "-t rpm --depends coreutils --depends shadow-utils"; do
      FPM_NAME=$(
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
        -C "$PKG_ROOT" \
        -p /out \
         | ruby -e 'puts (eval ARGF.read)[:path]' )

        echo "fpm created $FPM_NAME"
        NEW_NAME=$(echo "$FPM_NAME" | rev | sed "s/1-//" | rev)
        echo "renaming to ${NEW_NAME}"
        mv "${FPM_NAME}" "${NEW_NAME}"
    done
  fi

  #############################
  ######### Checksums #########
  #############################
  (cd /out && find . \( -name '*.deb' -o -name '*.rpm' -o -name '*.tar.gz' \) -exec sh -c 'md5sum {} > {}.md5 && sha256sum {} > {}.sha256' \;)
elif [ "$OS" == "windows" ]; then
  # Windows gets the binaries and the sample config file.
  rm -rf "$PKG_ROOT"
  mkdir -p "$PKG_ROOT"
  cp /ibin/*.exe "$PKG_ROOT"
  cp /isrc/etc/config.sample.toml "$PKG_ROOT/influxdb.conf"

  (cd /pkg && zip -9 -r "/out/influxdb-${VERSION}_${OS}_${ARCH}.zip" ./*)
  (cd /out && for f in *.zip; do
    md5sum "$f" > "$f.md5"
    sha256sum "$f" > "$f.sha256"
  done)
else
  >&2 echo "Unrecognized OS: $OS"
  exit 1
fi
