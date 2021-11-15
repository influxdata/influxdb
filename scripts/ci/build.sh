#!/usr/bin/env bash
set -exo pipefail

declare -r SCRIPT_DIR=$(cd $(dirname $0) > /dev/null && pwd)

function build_linux () {
    local tags=osusergo,netgo,static_build,assets,sqlite_foreign_keys,sqlite_json
    local cc
    case $(go env GOARCH) in
        amd64)
            cc=musl-gcc
            ;;
        arm64)
            cc=aarch64-unknown-linux-musl-gcc
            TAGS="$TAGS,noasm"
            ;;
        *)
            >&2 echo Error: Unknown arch $(go env GOARCH)
            exit 1
            ;;
    esac

    local -r commit=$(git rev-parse --short HEAD)
    local -r build_date=$(date -u +'%Y-%m-%dT%H:%M:%SZ')
    local -r extld="-fno-PIC -static -Wl,-z,stack-size=8388608"
    CGO_ENABLED=1 PKG_CONFIG=$(which pkg-config) CC=${cc} go build \
        -tags "$tags" \
        -buildmode pie \
        -ldflags "-s -w -X main.version=${2} -X main.commit=${commit} -X main.date=${build_date} -extldflags '${extld}'" \
        -o "${1}/" \
        "${3}"
}

function build_mac () {
    local -r commit=$(git rev-parse --short HEAD)
    local -r build_date=$(date -u +'%Y-%m-%dT%H:%M:%SZ')
    CGO_ENABLED=1 PKG_CONFIG=$(which pkg-config) CC=x86_64-apple-darwin16-clang go build \
        -tags assets,sqlite_foreign_keys,sqlite_json \
        -buildmode pie \
        -ldflags "-s -w -X main.version=${2} -X main.commit=${commit} -X main.date=${build_date}" \
        -o "${1}/" \
        "${3}"
}

function build_windows () {
    local -r commit=$(git rev-parse --short HEAD)
    local -r build_date=$(date -u +'%Y-%m-%dT%H:%M:%SZ')
    CGO_ENABLED=1 PKG_CONFIG=$(which pkg-config) CC=x86_64-w64-mingw32-gcc go build \
        -tags assets,sqlite_foreign_keys,sqlite_json,timetzdata \
        -buildmode exe \
        -ldflags "-s -w -X main.version=${2} -X main.commit=${commit} -X main.date=${build_date}" \
        -o "${1}/" \
        "${3}"
}

function main () {
    if [[ $# != 3 ]]; then
        >&2 echo Usage: $0 '<output-dir>' '<build-type>' '<pkg>'
        exit 1
    fi
    local -r out_dir=$1 build_type=$2 pkg=$3
    local -r version="$(${SCRIPT_DIR}/build-version.sh "$build_type")"

    rm -rf "$out_dir"
    mkdir -p "$out_dir"
    case $(go env GOOS) in
        linux)
            build_linux "$out_dir" "$version" "$pkg"
            ;;
        darwin)
            build_mac "$out_dir" "$version" "$pkg"
            ;;
        windows)
            build_windows "$out_dir" "$version" "$pkg"
            ;;
        *)
            >&2 echo Error: unknown OS $(go env GOOS)
            exit 1
            ;;
    esac
}

main ${@}
