#!/bin/bash
#
# Runs a command and waits until it or the parent process dies.
#
# See https://github.com/influxdata/influxdb_iox/issues/951
#
# Hack self destruction condition:
#   when we stop using ServerFixture::create_shared and similar test setup techniques
#   we can rely on RAII to properly tear down and remove this hack.
#

parent=$(ps -o ppid= $$)

cmd="$1"
shift

trap 'kill $(jobs -p)' EXIT

"${cmd}" "$@" &

child="$!"

echo child "${child}" >>/tmp/noorphan.log

check_pid() {
  kill -0 "$1" 2>/dev/null
}

while check_pid "${parent}" && check_pid "${child}"; do
  sleep 1
done
