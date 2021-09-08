#!/usr/bin/env bash
set -eo pipefail

declare -r CI_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"
declare -r SCRIPT_DIR="$(dirname "$CI_SCRIPT_DIR")"
declare -r ROOT_DIR="$(dirname "$SCRIPT_DIR")"
declare -r DIST_DIR="$ROOT_DIR/dist"

declare -r NFPM_VERSION=2.6.0
declare -r NFPM_ARCHIVE=nfpm_${NFPM_VERSION}_Linux_x86_64.tar.gz
declare -r NFPM_URL=https://github.com/goreleaser/nfpm/releases/download/v${NFPM_VERSION}/${NFPM_ARCHIVE}
declare -r NFPM_SHA=5b0bc0fd6fe6dc7cf0e1cf8dd1220fcbe6039720bf78c6babf1d98589c4598d4

function install_nfpm() {
    wget ${NFPM_URL}
    echo "${NFPM_SHA}  ${NFPM_ARCHIVE}" | sha256sum --check || { echo "Checksum problem!"; exit 1; }
    tar xzvf ${NFPM_ARCHIVE} nfpm
    install nfpm ${GOPATH}/bin
    rm nfpm ${NFPM_ARCHIVE}
}

function create_package() {
    local -r arch=$1 version=$2 bindir=$3 packager=$4
    local version_shorthand
    case ${version} in
        *SNAPSHOT*)
            version_shorthand=SNAPSHOT
            ;;
        *nightly*)
            version_shorthand=nightly
            ;;
        *)
            version_shorthand=${version}
            ;;
    esac
    local package_name
    case ${packager} in
        deb)
            package_name=influxdb2-${version_shorthand}-${arch}.deb
            ;;
        rpm)
            package_name=influxdb2-${version_shorthand}.${arch}.rpm
            ;;
        *)
            >&2 echo Unknown packager ${packager}!
            exit 1
    esac

    cat <<EOF > nfpm.yml
name: influxdb2

arch: ${arch}
version: ${version}

vendor: InfluxData
homepage: https://influxdata.com
maintainer: support@influxdb.com
description: Distributed time-series database
license: MIT
conflicts:
  - influxdb
depends:
  - curl
recommends:
  - influxdb2-cli
contents:
  - src: ${bindir}/influxd
    dst: /usr/bin/influxd
  - src: ${SCRIPT_DIR}/init.sh
    dst: /usr/lib/influxdb/scripts/init.sh
  - src: ${SCRIPT_DIR}/influxdb.service
    dst: /usr/lib/influxdb/scripts/influxdb.service
  - src: ${SCRIPT_DIR}/logrotate
    dst: /etc/logrotate.d/influxdb
  - src: ${SCRIPT_DIR}/influxdb2-upgrade.sh
    dst: /usr/share/influxdb/influxdb2-upgrade.sh
  - src: ${SCRIPT_DIR}/influxd-systemd-start.sh
    dst: /usr/lib/influxdb/scripts/influxd-systemd-start.sh
scripts:
  preinstall: ${SCRIPT_DIR}/pre-install.sh
  postinstall: ${SCRIPT_DIR}/post-install.sh
  postremove: ${SCRIPT_DIR}/post-uninstall.sh
EOF
    nfpm package -f $(pwd)/nfpm.yml -p ${packager} -t ${bindir}/${package_name}
    rm nfpm.yml
}

function main() {
    local -r version=$(${DIST_DIR}/influxd_linux_amd64/influxd version | cut -f 2 -d ' ')

    install_nfpm
    create_package amd64 ${version} ${DIST_DIR}/influxd_linux_amd64 deb
    create_package arm64 ${version} ${DIST_DIR}/influxd_linux_arm64 deb
    create_package x86_64 ${version} ${DIST_DIR}/influxd_linux_amd64 rpm
    create_package aarch64 ${version} ${DIST_DIR}/influxd_linux_arm64 rpm
}

main ${@}
