#!/usr/bin/env bash
set -eo pipefail

function setup_linux () {
    sudo apt-get update
    sudo apt-get install -y --no-install-recommends \
        bzr \
        clang \
        libprotobuf-dev \
        make \
        musl-tools \
        pkg-config \
        protobuf-compiler
}

function setup_mac () {
    # `python@3.9` comes pre-installed on the macOS executor, and it includes the `six` package.
    # The protobuf formula hasn't caught up yet, and depends on the standalone `six` formula.
    # Attempting to install the `six` formula alongside `python@3.9` causes a linking error because
    # brew doesn't overwrite existing files.
    #
    # The brew help-text suggests deleting the conflicting file before running the `install`.
    local -r six_file=/usr/local/lib/python3.9/site-packages/six.py
    if [[ -f ${six_file} ]]; then
        rm ${six_file}
    fi

    HOMEBREW_NO_AUTO_UPDATE=1 brew install \
        bazaar \
        pkg-config \
        protobuf \
        wget
}

function setup_windows () {
    choco install \
        bzr \
        llvm \
        make \
        pkgconfiglite \
        protoc \
        wget

    # rustc depends on a version of libgcc_eh that isn't present in the latest mingw.
    choco install mingw --version=8.1.0
}

function main () {
    case $(uname) in
        Linux)
            setup_linux
            ;;
        Darwin)
            setup_mac
            ;;
        MSYS_NT*)
            setup_windows
            ;;
        *)
            >&2 echo Error: unknown OS $(uname)
            exit 1
            ;;
    esac
}

main ${@}
