#!/usr/bin/env bash
set -euo pipefail

function main () {
    if [[ $# != 1 ]]; then
        >&2 echo Usage: $0 '<build-type>'
        exit 1
    fi
    local -r build_type=$1

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

    echo "$version"
}

main ${@}
