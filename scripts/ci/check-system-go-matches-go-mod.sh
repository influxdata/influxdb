#!/usr/bin/env bash
set -eo pipefail

# Check that the same major/minor version of go is used in the mod file as on CI to prevent
# the two from accidentally getting out-of-sync.
function main () {
    local -r go_version=$(go version | sed -n 's/^.*go\([0-9]*.[0-9]*\).*$/\1/p')
    local -r go_mod_go_version=$(sed -n 's/^go \([0-9]*.[0-9]*\).*$/\1/p' < go.mod)
    if [ "$go_version" != "$go_mod_go_version" ]; then
        >&2 echo Error: unexpected difference in go version:
        >&2 echo "go version: $go_version, go.mod version: $go_mod_go_version"
        exit 1
    fi
}

main
