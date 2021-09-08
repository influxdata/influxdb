#!/usr/bin/env bash
set -eo pipefail

function build_linux () {
    TAGS=osusergo,netgo,static_build,assets,sqlite_foreign_keys,sqlite_json
    if [[ $(go env GOARCH) != amd64 ]]; then
        TAGS="$TAGS,noasm"
    fi

    local -r commit=$(git rev-parse --short HEAD)
    local -r build_date=$(date -u +'%Y-%m-%dT%H:%M:%SZ')
    local -r extld="-fno-PIC -static -Wl,-z,stack-size=8388608"
    PKG_CONFIG=$(which pkg-config) CC=musl-gcc go build \
        -tags "$TAGS" \
        -buildmode pie \
        -ldflags "-s -w -X main.version=${2} -X main.commit=${commit} -X main.date=${build_date} -extldflags '${extld}'" \
        -o "${1}/" \
        ./cmd/influxd/
}

function build_mac () {
    local -r commit=$(git rev-parse --short HEAD)
    local -r build_date=$(date -u +'%Y-%m-%dT%H:%M:%SZ')
    PKG_CONFIG=$(which pkg-config) go build \
        -tags assets,sqlite_foreign_keys,sqlite_json \
        -buildmode pie \
        -ldflags "-s -w -X main.version=${2} -X main.commit=${commit} -X main.date=${build_date}" \
        -o "${1}/" \
        ./cmd/influxd/
}

function build_windows () {
    local -r commit=$(git rev-parse --short HEAD)
    local -r build_date=$(date -u +'%Y-%m-%dT%H:%M:%SZ')
    PKG_CONFIG=$(which pkg-config) CC=x86_64-w64-mingw32-gcc go build \
        -tags assets,sqlite_foreign_keys,sqlite_json \
        -buildmode exe \
        -ldflags "-s -w -X main.version=${2} -X main.commit=${commit} -X main.date=${build_date}" \
        -o "${1}/" \
        ./cmd/influxd/
}

function main () {
    if [[ $# != 2 ]]; then
        >&2 echo Usage: $0 '<output-dir>' '<build-type>'
        exit 1
    fi
    local -r out_dir=$1 build_type=$2
    local version
    case "$build_type" in
      release)
        version=$(git describe --tags --abbrev=0 --exact-match)
        ;;
      nightly)
        version=$(git describe --tags --abbrev=0)+nightly.$(date +%Y.%m.%d)
        ;;
      snapshot)
        version=$(git describe --tags --abbrev=0)+SNAPSHOT.$(git rev-parse --short HEAD)
        ;;
      *)
        >&2 echo Error: unknown build type "'$build_type'"
        ;;
    esac
    if [ -z "$version" ]; then
      >&2 echo Error: "couldn't" compute version for build type "'$build_type'"
      exit 1
    fi

    rm -rf "$out_dir"
    mkdir -p "$out_dir"
    case $(go env GOOS) in
        linux)
            build_linux "$out_dir" "$version"
            ;;
        darwin)
            build_mac "$out_dir" "$version"
            ;;
        windows)
            build_windows "$out_dir" "$version"
            ;;
        *)
            >&2 echo Error: unknown OS $(go env GOOS)
            exit 1
            ;;
    esac

    "${out_dir}/influxd" version
}

main ${@}
