#!/usr/bin/env bash
set -e

declare -r SCRIPT_DIR=$(cd $(dirname ${0}) >/dev/null 2>&1 && pwd)
declare -r ROOT_DIR=$(dirname ${SCRIPT_DIR})

declare -r GOGO_PATH=github.com/gogo/protobuf

if [ -d "${ROOT_DIR}/vendor" ]; then
  echo "${ROOT_DIR}/vendor/${GOGO_PATH}"
else
  go list -f '{{ .Dir }}' -m ${GOGO_PATH}
fi
