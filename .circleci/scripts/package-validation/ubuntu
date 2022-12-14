#!/bin/bash
set -o errexit \
    -o nounset \
    -o pipefail

path="$(dirname "$(realpath "${BASH_SOURCE[0]}")")"

"${path}/validate" deb "${1}"
