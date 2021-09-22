#!/usr/bin/env bash
set -exo pipefail

function test_package () {
    local -r pkg="$1" bin_dir="$2" result_dir="$3"

    local -r test_file="${bin_dir}/${pkg}.test"
    if [ ! -f "$test_file" ]; then
        return
    fi

    out_dir="${result_dir}/${pkg}"
    mkdir -p "${out_dir}"

    # Run test files from within their original packages so any relative references
    # to data files resolve properly.
    local source_dir="${pkg##github.com/influxdata/influxdb/v2}"
    source_dir="${source_dir##/}"
    if [ -z "$source_dir" ]; then
        source_dir="."
    fi
    (
        set +e
        cd "$source_dir"
        GOVERSION="$(cat ${bin_dir}/go.version)" "${bin_dir}/gotestsum" --junitfile "${out_dir}/report.xml" --raw-command -- \
            "${bin_dir}/test2json" -t -p "$pkg" "$test_file" -test.v
        if [ $? != 0 ]; then
            echo 1 > "${result_dir}/rc"
        fi
    )
}

function main () {
    if [[ $# != 2 ]]; then
        >&2 echo Usage: $0 '<test-bin-dir>' '<result-dir>'
        exit 1
    fi
    local -r bin_dir="$1" result_dir="$2"

    mkdir -p "$result_dir"

    local -r test_packages="$(cat "${bin_dir}/tests.list" | circleci tests split --split-by=timings --timings-type=classname)"

    echo 0 > "${result_dir}/rc"
    for pkg in ${test_packages[@]}; do
        test_package "$pkg" "$bin_dir" "$result_dir"
    done

    exit $(cat "${result_dir}/rc")
}

main ${@}
