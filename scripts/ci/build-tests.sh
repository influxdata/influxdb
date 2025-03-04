#!/usr/bin/env bash
set -exo pipefail

function build_linux () {
    local tags=osusergo,netgo,sqlite_foreign_keys,sqlite_json,static_build
    local cc
    case $(go env GOARCH) in
        amd64)
            cc=$(xcc linux x86_64)
            ;;
        arm64)
            cc=$(xcc linux aarch64)
            tags="$tags,noasm"
            ;;
        *)
            >&2 echo Error: Unknown arch $(go env GOARCH)
            exit 1
            ;;
    esac

    local -r extld="-fno-PIC -static -Wl,-z,stack-size=8388608"
    CGO_ENABLED=1 PKG_CONFIG=$(which pkg-config) CC="${cc}" go-test-compile \
        -tags "$tags" -o "${1}/" -ldflags "-extldflags '$extld'" -x ./...
}

function build_mac () {
    CGO_ENABLED=1 PKG_CONFIG=$(which pkg-config) CC="$(xcc darwin)" go-test-compile \
        -tags sqlite_foreign_keys,sqlite_json -o "${1}/" -x ./...
}

function build_windows () {
  export CC="$(xcc windows)"
  export CGO_LDFLAGS="-lntdll -ladvapi32 -lkernel32 -luserenv -lws2_32"

  CGO_ENABLED=1 PKG_CONFIG=$(which pkg-config)  go-test-compile \
        -tags sqlite_foreign_keys,sqlite_json,timetzdata \
        -ldext "${CC}" -o "${1}/" ./...
}

function build_test_tools () {
    # Copy pre-built gotestsum out of the cross-builder.
    local ext=""
    if [ "$(go env GOOS)" = windows ]; then
        ext=".exe"
    fi
    cp "/usr/local/bin/gotestsum_$(go env GOOS)_$(go env GOARCH)${ext}" "$1/gotestsum${ext}"

    # Build test2json from the installed Go distribution.
    CGO_ENABLED=0 go build -o "${1}/" -ldflags="-s -w" -x cmd/test2json
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
