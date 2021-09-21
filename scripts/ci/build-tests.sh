#!/usr/bin/env bash
set -exo pipefail

declare -r GOTESTSUM_VERSION=1.7.0

function build_linux () {
    local tags=osusergo,netgo,sqlite_foreign_keys,sqlite_json,static_build
    local cc
    case $(go env GOARCH) in
        amd64)
            cc=musl-gcc
            ;;
        arm64)
            cc=aarch64-unknown-linux-musl-gcc
            tags="$tags,noasm"
            ;;
        *)
            >&2 echo Error: Unknown arch $(go env GOARCH)
            exit 1
            ;;
    esac

    local -r extld="-fno-PIC -static -Wl,-z,stack-size=8388608"
    CGO_ENABLED=1 PKG_CONFIG=$(which pkg-config) CC=${cc} go-test-compile \
        -tags "$tags" -o "${1}/" -ldflags "-extldflags '$extld'" ./...
}

function build_mac () {
    CGO_ENABLED=1 PKG_CONFIG=$(which pkg-config) CC=x86_64-apple-darwin16-clang go-test-compile \
        -tags sqlite_foreign_keys,sqlite_json -o "${1}/" ./...
}

function build_windows () {
    CGO_ENABLED=1 PKG_CONFIG=$(which pkg-config) CC=x86_64-w64-mingw32-gcc go-test-compile \
        -tags sqlite_foreign_keys,sqlite_json,timetzdata -o "${1}/" ./...
}

function build_test_tools () {
    # Download gotestsum from its releases (faster than building it).
    local -r gotestsum_url=https://github.com/gotestyourself/gotestsum/releases/download/v${GOTESTSUM_VERSION}/gotestsum_${GOTESTSUM_VERSION}_$(go env GOOS)_$(go env GOARCH).tar.gz
    curl -L "$gotestsum_url" | tar xz -C "${1}/"

    # Build test2json from the installed Go distribution.
    CGO_ENABLED=0 go build -o "${1}/" -ldflags="-s -w" cmd/test2json
}

function write_test_metadata () {
    # Write version that should be reported in test results.
    echo "$(go env GOVERSION) $(go env GOOS)/$(go env GOARCH)" > "${1}/go.version"

    # Write list of all packages.
    go list ./... > "${1}/tests.list"
}

function main () {
    if [[ $# != 1 ]]; then
        >&2 echo Usage: $0 '<output-dir>'
        exit 1
    fi
    local -r out_dir="$1"

    rm -rf "$out_dir"
    mkdir -p "$out_dir"
    case $(go env GOOS) in
        linux)
            build_linux "$out_dir"
            ;;
        darwin)
            build_mac "$out_dir"
            ;;
        windows)
            build_windows "$out_dir"
            ;;
        *)
            >&2 echo Error: unknown OS $(go env GOOS)
            exit 1
            ;;
    esac

    # Build gotestsum and test2json so downstream jobs can use it without needing `go`.
    build_test_tools "$out_dir"
    # Write other metadata needed for testing.
    write_test_metadata "$out_dir"
}

main ${@}
