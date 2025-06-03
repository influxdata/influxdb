#!/bin/bash
set -e
for (( ; ; ))
do
  rm -rf /tmp/influxdb*
  # TEST_LOG=1 RUST_LOG=debug LOG_FILTER=debug RUST_LOG_SPAN_EVENTS=full RUST_BACKTRACE=full cargo nextest run --workspace --failure-output immediate-final --no-fail-fast
  TEST_LOG=1 RUST_LOG=debug LOG_FILTER=debug RUST_LOG_SPAN_EVENTS=full RUST_BACKTRACE=full cargo nextest run test_check_mem_and_force_snapshot --workspace --failure-output immediate-final --no-fail-fast
  echo "sleeping"
  sleep 1
done
