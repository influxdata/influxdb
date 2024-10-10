#!/bin/bash
set -e
for (( ; ; ))
do
  rm -rf /tmp/influxdb*
  TEST_LOG=1 RUST_LOG=info RUST_LOG_SPAN_EVENTS=full RUST_BACKTRACE=1 cargo nextest run --workspace --failure-output immediate-final --no-fail-fast
  echo "sleeping"
  sleep 1
done
