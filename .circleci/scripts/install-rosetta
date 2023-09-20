#!/bin/bash
set -euo pipefail

if [[ "${MACHTYPE}" == "arm64-apple-darwin"* ]]
then
  /usr/sbin/softwareupdate --install-rosetta --agree-to-license
fi
