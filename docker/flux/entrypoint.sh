#!/usr/bin/env sh
set -e

if [ "${1:0:1}" = '-' ]; then
    set -- fluxd "$@"
fi

exec "$@"
