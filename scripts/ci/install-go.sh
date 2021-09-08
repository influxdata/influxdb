#!/usr/bin/env bash
set -eo pipefail

declare -r GO_VERSION=1.17

# Hashes are from the table at https://golang.org/dl/
function go_hash () {
    case $1 in
        linux_amd64)
            echo 6bf89fc4f5ad763871cf7eac80a2d594492de7a818303283f1366a7f6a30372d
            ;;
        linux_arm64)
            echo 01a9af009ada22122d3fcb9816049c1d21842524b38ef5d5a0e2ee4b26d7c3e7
            ;;
        mac_amd64)
            echo 355bd544ce08d7d484d9d7de05a71b5c6f5bc10aa4b316688c2192aeb3dacfd1
            ;;
        windows_amd64)
            echo 2a18bd65583e221be8b9b7c2fbe3696c40f6e27c2df689bbdcc939d49651d151
            ;;
    esac
}

function install_go_linux () {
    local -r arch=$(dpkg --print-architecture)
    ARCHIVE=go${GO_VERSION}.linux-${arch}.tar.gz
    wget https://golang.org/dl/${ARCHIVE}
    echo "$(go_hash linux_${arch})  ${ARCHIVE}" | sha256sum --check --
    tar -C $1 -xzf ${ARCHIVE}
    rm ${ARCHIVE}
}

function install_go_mac () {
    ARCHIVE=go${GO_VERSION}.darwin-amd64.tar.gz
    wget https://golang.org/dl/${ARCHIVE}
    echo "$(go_hash mac_amd64)  ${ARCHIVE}" | shasum -a 256 --check -
    tar -C $1 -xzf ${ARCHIVE}
    rm ${ARCHIVE}
}

function install_go_windows () {
    ARCHIVE=go${GO_VERSION}.windows-amd64.zip
    wget https://golang.org/dl/${ARCHIVE}
    echo "$(go_hash windows_amd64)  ${ARCHIVE}" | sha256sum --check --
    unzip -qq -d $1 ${ARCHIVE}
    rm ${ARCHIVE}
}

function main () {
    if [[ $# != 1 ]]; then
        >&2 echo Usage: $0 '<install-dir>'
        exit 1
    fi
    local -r install_dir=$1

    rm -rf "$install_dir"
    mkdir -p "$install_dir"
    case $(uname) in
        Linux)
            install_go_linux "$install_dir"
            ;;
        Darwin)
            install_go_mac "$install_dir"
            ;;
        MSYS_NT*)
            install_go_windows "$install_dir"
            ;;
        *)
            >&2 echo Error: unknown OS $(uname)
            exit 1
            ;;
    esac

    "${install_dir}/go/bin/go" version
}

main ${@}
