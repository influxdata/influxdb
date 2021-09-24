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

function main () {
    if [[ $# != 1 ]]; then
        >&2 echo Usage: $0 '<output-dir>'
        exit 1
    fi

    local -r gotestsum_archive="gotestsum_${GOTESTSUM_VERSION}_$(go env GOOS)_$(go env GOARCH).tar.gz"
    local -r gotestsum_url="https://github.com/gotestyourself/gotestsum/releases/download/v${GOTESTSUM_VERSION}/${gotestsum_archive}"

    curl -L "$gotestsum_url" -O
    echo "$(gotestsum_expected_sha)  ${gotestsum_archive}" | sha256sum --check --
    tar xzf "$gotestsum_archive" -C "${1}/"
    rm "$gotestsum_archive"
}

main "${@}"
