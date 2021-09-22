#!/usr/bin/env bash
set -exo pipefail

declare -r GOTESTSUM_VERSION=1.7.0

# Values are from https://github.com/gotestyourself/gotestsum/releases/download/v${VERSION}/gotestsum-${VERSION}-checksums.txt
function gotestsum_expected_sha () {
    case "$(go env GOOS)_$(go env GOARCH)" in
        linux_amd64)
            echo b5c98cc408c75e76a097354d9487dca114996e821b3af29a0442aa6c9159bd40
            ;;
        linux_arm64)
            echo ee57c91abadc464a7cd9f8abbffe94a673aab7deeb9af8c17b96de4bb8f37e41
            ;;
        darwin_amd64)
            echo a8e2351604882af1a67601cbeeacdcfa9b17fc2f6fbac291cf5d434efdf2d85b
            ;;
        windows_amd64)
            echo 7ae12ddb171375f0c14d6a09dd27a5c1d1fc72edeea674e3d6e7489a533b40c1
            ;;
        *)
            >&2 echo Error: Unsupported OS/arch pair for gotestsum: "$(go env GOOS)_$(go env GOARCH)"
            exit 1
            ;;
    esac
}

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
    local -r gotestsum_archive="gotestsum_${GOTESTSUM_VERSION}_$(go env GOOS)_$(go env GOARCH).tar.gz"
    local -r gotestsum_url="https://github.com/gotestyourself/gotestsum/releases/download/v${GOTESTSUM_VERSION}/${gotestsum_archive}"

    curl -L "$gotestsum_url" -O
    echo "$(gotestsum_expected_sha)  ${gotestsum_archive}" | sha256sum --check --
    tar xzf "$gotestsum_archive" -C "${1}/"
    rm "$gotestsum_archive"

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
