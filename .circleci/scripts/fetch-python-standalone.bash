#!/bin/bash
set -euo pipefail

# See https://github.com/astral-sh/python-build-standalone/releases
# USAGE:
# fetch-python-standalone.bash <dir> <RELEASE DATE> <RELEASE VERSION>
#
# Eg:
# $ fetch-python-standalone.bash ./python-artifacts 20250106 3.11.11
#
# This script is meant to be called by CircleCI such that the specified <dir>
# is persisted to a workspace that is later attached at /tmp/workspace/<dir>.
# In this manner, build script can do something like:
#   PYO3_CONFIG_FILE=/tmp/workspace/<dir>/pyo3_config_file.txt cargo build...

tmp=$(mktemp -d)
pushd $tmp
readonly INPUT_DIR="$1"
readonly DOWNLOAD_DIR="${tmp}/$1"

# URLs are constructed from this. Eg:
# https://github.com/astral-sh/.../<PBS_DATE>/cpython-<PBS_VERSION>+<PBS_DATE>-<ARCH>...
readonly PBS_DATE="$2"
readonly PBS_VERSION="$3"
readonly PBS_MAJ_MIN=${PBS_VERSION%.*}
readonly PBS_TOP_DIR="/tmp/workspace"

# Official influxdb3 builds use python-build-standalone since it:
# - is built to run well as an embedded interpreter
# - has a good upstream maintenance story (https://github.com/astral-sh) with
#   lots of users and corporate sponsor
# - should deliver a consistent experience across OSes and architectures
#
# python-build-standalone provides many different builds. Official influxdb3
# build targets:
# - aarch64-apple-darwin
# - aarch64-unknown-linux-gnu
# - x86_64-unknown-linux-gnu
# - x86_64-pc-windows-msvc
#
# Note: musl builds of python-build-standablone currently (as of 2025-02-04)
# have limitations:
# - don't support importing bre-built python wheels (must compile and link 3rd
#   party extensions into the binary (influxdb3)
# - historical performance issues with python and musl
# - availability limited to x86_64 (no aarch64)
#
# References
# - https://github.com/astral-sh/python-build-standalone/blob/main/docs/distributions.rst
# - https://github.com/astral-sh/python-build-standalone/blob/main/docs/running.rst
# - https://edu.chainguard.dev/chainguard/chainguard-images/about/images-compiled-programs/glibc-vs-musl/#python-builds
# - https://pythonspeed.com/articles/alpine-docker-python/
readonly TARGETS="aarch64-apple-darwin aarch64-unknown-linux-gnu x86_64-unknown-linux-gnu x86_64-pc-windows-msvc"

fetch() {
    target="$1"
    suffix="${2}"
    if [ "${suffix}" = "full.tar.zst" ]; then
        if [ "${target}" = "x86_64-pc-windows-msvc" ]; then
            suffix="pgo-${2}"
        else
            suffix="debug-${2}"
        fi
    fi
    binary="cpython-${PBS_VERSION}+${PBS_DATE}-${target}-${suffix}"
    url="https://github.com/astral-sh/python-build-standalone/releases/download/${PBS_DATE}/${binary}"

    echo "Downloading ${binary}"
    curl --proto '=https' --tlsv1.2 -sS -L "$url" -o "${DOWNLOAD_DIR}/${binary}"

    echo "Downloading ${binary}.sha256"
    curl --proto '=https' --tlsv1.2 -sS -L "${url}.sha256" -o "${DOWNLOAD_DIR}/${binary}.sha256"
    dl_sha=$(cut -d ' ' -f 1 "${DOWNLOAD_DIR}/${binary}.sha256")
    if [ -z "$dl_sha" ]; then
        echo "Could not find properly formatted SHA256 in '${DOWNLOAD_DIR}/${binary}.sha256'"
        exit 1
    fi

    printf "Verifying %s: " "${binary}"
    ch_sha=$(sha256sum "${DOWNLOAD_DIR}/${binary}" | cut -d ' ' -f 1)
    if [ "$ch_sha" = "$dl_sha" ]; then
        echo "OK"
    else
        echo "ERROR (${ch_sha} != ${dl_sha})"
        exit 1
    fi

    echo "Unpacking ${binary} to '${DOWNLOAD_DIR}'"
    UNPACK_DIR="${DOWNLOAD_DIR}/${target}"
    if [ "${target}" = "x86_64-pc-windows-msvc" ]; then
        UNPACK_DIR="${DOWNLOAD_DIR}/x86_64-pc-windows-gnu"
    fi
    mkdir "${UNPACK_DIR}" 2>/dev/null || true
    if [[ "${suffix}" = *full.tar.zst ]]; then
        # we only need the licensing from the full distribution
        tar -C "${UNPACK_DIR}" --zstd -xf "${DOWNLOAD_DIR}/${binary}" python/PYTHON.json python/licenses
        mv "${UNPACK_DIR}/python/PYTHON.json" "${UNPACK_DIR}/python/licenses"
    else
        tar -C "${UNPACK_DIR}" -zxf "${DOWNLOAD_DIR}/${binary}"
    fi

    echo "Removing ${binary}"
    rm -f "${DOWNLOAD_DIR}/${binary}" "${DOWNLOAD_DIR}/${binary}.sha256"

    if [[ "${suffix}" = *install_only_stripped.tar.gz ]]; then
        echo "Creating ${UNPACK_DIR}/pyo3_config_file.txt"
        PYO3_CONFIG_FILE="${UNPACK_DIR}/pyo3_config_file.txt"
        PBS_DIR="${PBS_TOP_DIR}"/$(basename "${DOWNLOAD_DIR}")/$(basename "${UNPACK_DIR}")
        if [ "${target}" = "x86_64-pc-windows-msvc" ]; then
            cat > "${PYO3_CONFIG_FILE}" <<EOM
implementation=CPython
version=${PBS_MAJ_MIN}
shared=true
abi3=false
lib_name=python${PBS_MAJ_MIN//./}
lib_dir=${PBS_DIR}/python/libs
executable=${PBS_DIR}/python/python.exe
pointer_width=64
build_flags=
suppress_build_script_link_lines=false
EOM
        else
            cat > "${PYO3_CONFIG_FILE}" <<EOM
implementation=CPython
version=${PBS_MAJ_MIN}
shared=true
abi3=false
lib_name=python${PBS_MAJ_MIN}
lib_dir=${PBS_DIR}/python/lib
executable=${PBS_DIR}/python/bin/python${PBS_MAJ_MIN}
pointer_width=64
build_flags=
suppress_build_script_link_lines=false
EOM
        fi

        echo "Creating ${UNPACK_DIR}/python/LICENSE.md"
        cat > "${UNPACK_DIR}/python/LICENSE.md" <<EOM
This version of InfluxDB is built against and uses the redistributable build of
'python-build-standalone' downloaded from:

  ${url}
EOM
    else
        echo "Adding licenses/ provenance to ${UNPACK_DIR}/python/LICENSE.md"
        cat >> "${UNPACK_DIR}/python/LICENSE.md" <<EOM

License information for this 'python-build-standalone' build can be found in
the licenses/ sub-folder. The contents of this folder was extracted from:

  ${url}

References:
- https://github.com/astral-sh/python-build-standalone/releases/tag/${PBS_DATE}
- https://github.com/astral-sh/python-build-standalone/blob/${PBS_DATE}/docs/running.rst#licensing
EOM
    fi

    echo
}

mkdir -p "${DOWNLOAD_DIR}" # $(mktemp -d)/python-artifacts
for t in $TARGETS ; do
    fetch "$t" "install_only_stripped.tar.gz"   # for runtime
    fetch "$t" "full.tar.zst"                   # for licenses
done

# This speeds up CircleCI
echo "Creating '${DOWNLOAD_DIR}/all.tar.gz'"
cd "${DOWNLOAD_DIR}"
tar -zcf ./.all.tar.gz ./[a-z]*
popd # /tmp/workspace/$CIRCLE_PIPELINE_ID
mkdir -p $INPUT_DIR # /tmp/workspace/$CIRCLE_PIPELINE_ID/python-artifacts
mv $DOWNLOAD_DIR/.all.tar.gz $INPUT_DIR/all.tar.gz
rm -rf $tmp
