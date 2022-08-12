#!/usr/bin/env bash
set -exo pipefail

declare -r LINUX_EXTLD="-fno-PIC -static-pie -Wl,-z,stack-size=8388608"

function main () {
    if [[ $# != 3 ]]; then
        >&2 echo Usage: $0 '<output-dir>' '<build-type>' '<pkg>'
        exit 1
    fi
    local -r out_dir=$1 build_type=$2 pkg=$3
    local -r version="$(build-version.sh "$build_type")"

    mkdir -p "$out_dir"

    local -r commit=$(git rev-parse --short HEAD)
    local -r build_date=$(date -u +'%Y-%m-%dT%H:%M:%SZ')

    # NOTE: This code is purposefully repetitive, to enable easier copy-paste of individual build commands.
    local -r os_arch="$(go env GOOS)_$(go env GOARCH)"
    case "$os_arch" in
        linux_amd64)
            export CC="$(xcc linux x86_64)"
            CGO_ENABLED=1 PKG_CONFIG=$(which pkg-config) go build \
                -tags assets,sqlite_foreign_keys,sqlite_json,static_build,noasm \
                -buildmode=pie \
                -ldflags "-s -w -X main.version=${version} -X main.commit=${commit} -X main.date=${build_date} -linkmode=external -extld=${CC} -extldflags '${LINUX_EXTLD}'" \
                -o "$out_dir/" \
                "$pkg"
            ;;
        linux_arm64)
            export CC="$(xcc linux aarch64)"
            CGO_ENABLED=1 PKG_CONFIG=$(which pkg-config) go build \
                -tags assets,sqlite_foreign_keys,sqlite_json,static_build,noasm \
                -buildmode=pie \
                -ldflags "-s -w -X main.version=${version} -X main.commit=${commit} -X main.date=${build_date} -linkmode=external -extld=${CC} -extldflags '${LINUX_EXTLD}'" \
                -o "$out_dir/" \
                "$pkg"
            ;;
        darwin_amd64)
            export CC="$(xcc darwin)"
            CGO_ENABLED=1 PKG_CONFIG=$(which pkg-config) go build \
                -tags assets,sqlite_foreign_keys,sqlite_json \
                -buildmode pie \
                -ldflags "-s -w -X main.version=${version} -X main.commit=${commit} -X main.date=${build_date}" \
                -o "$out_dir/" \
                "$pkg"
            ;;
        windows_amd64)
            export CC="$(xcc windows)"
            CGO_ENABLED=1 PKG_CONFIG=$(which pkg-config) go build \
                -tags assets,sqlite_foreign_keys,sqlite_json,timetzdata \
                -buildmode exe \
                -ldflags "-s -w -X main.version=${version} -X main.commit=${commit} -X main.date=${build_date}" \
                -o "$out_dir/" \
                "$pkg"
            ;;
        *)
            >&2 echo Error: unsupported OS_ARCH pair "'$os_arch'"
            exit 1
            ;;
    esac
}

main ${@}
