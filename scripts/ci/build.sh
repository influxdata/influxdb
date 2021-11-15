#!/usr/bin/env bash
set -exo pipefail

declare -r SCRIPT_DIR=$(cd $(dirname $0) > /dev/null && pwd)

declare -r LINUX_EXTLD="-fno-PIC -static -Wl,-z,stack-size=8388608"

function main () {
    if [[ $# != 3 ]]; then
        >&2 echo Usage: $0 '<output-dir>' '<build-type>' '<pkg>'
        exit 1
    fi
    local -r out_dir=$1 build_type=$2 pkg=$3
    local -r version="$(${SCRIPT_DIR}/build-version.sh "$build_type")"

    mkdir -p "$out_dir"

    local -r commit=$(git rev-parse --short HEAD)
    local -r build_date=$(date -u +'%Y-%m-%dT%H:%M:%SZ')

    local tags=assets,sqlite_foreign_keys,sqlite_json
    local buildmode=pie
    local ldflags="-s -w -X main.version=${version} -X main.commit=${commit} -X main.date=${build_date}"
    local cc

    local -r os_arch="$(go env GOOS)_$(go env GOARCH)"
    case "$os_arch" in
        linux_amd64)
            cc=musl-gcc
            ldflags="$ldflags -extldflags '$LINUX_EXTLD'"
            ;;
        linux_arm64)
            cc=aarch64-unknown-linux-musl-gcc
            ldflags="$ldflags -extldflags '$LINUX_EXTLD'"
            tags="$tags,noasm"
            ;;
        darwin_amd64)
            cc=x86_64-apple-darwin16-clang
            ;;
        windows_amd64)
            cc=x86_64-w64-mingw32-gcc
            tags="$tags,timetzdata"
            buildmode=exe
            ;;
        *)
            >&2 echo Error: unsupported OS_ARCH pair "'$os_arch'"
            exit 1
            ;;
    esac

    CGO_ENABLED=1 PKG_CONFIG=$(which pkg-config) CC=${cc} go build \
        -tags "$tags" \
        -buildmode "$buildmode" \
        -ldflags "$ldflags" \
        -o "$out_dir/" \
        "$pkg"
}

main ${@}
