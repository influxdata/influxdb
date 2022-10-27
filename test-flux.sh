#!/bin/bash
set -eu -o pipefail
readonly GO=${GO:-go}

log() {
  local now
  now=$(date '+%Y/%m/%d %H:%M:%S')
  echo "[${now}]" "$@"
}

determine_flux_revision() {
  local version revision
  version=$("$GO" list -m -f '{{.Version}}' github.com/influxdata/flux)
  revision=$(printf "%s" "${version}" | cut -d- -f 3)
  if [[ ${revision} != "" ]]; then
    printf "%s\n" "${revision}"
  else
    printf "%s\n" "${version}"
  fi
}

download_flux_archive() {
  local revision
  revision=$(determine_flux_revision)
  log "Downloading flux archive (${revision})..."
  curl -sLo flux.zip "https://github.com/influxdata/flux/archive/${revision}.zip"
}

build_test_harness() {
  log "Building test harness..."
  "$GO" build -o fluxtest ./internal/cmd/fluxtest-harness-influxdb
}

skipped_tests() {
  doc=$(cat <<ENDSKIPS
# Tests skipped because a feature flag must be enabled
# the flag is: removeRedundantSortNodes
remove_sort
remove_sort_more_columns
remove_sort_aggregate
remove_sort_selector
remove_sort_filter_range
remove_sort_aggregate_window
remove_sort_join

# Other skipped tests
align_time
buckets
covariance
cumulative_sum_default
cumulative_sum_noop
cumulative_sum
difference_columns
fill
fill_bool
fill_float
fill_time
fill_int
fill_uint
fill_string
group
group_nulls
histogram_normalize
histogram_quantile_minvalue
histogram_quantile
histogram
key_values_host_name
secrets
set
shapeDataWithFilter
shapeData
shift_negative_duration
unique
window_null

# https://github.com/influxdata/influxdb/issues/23757
# Flux acceptance tests for group |> first (and last)
push_down_group_one_tag_first
push_down_group_all_filter_field_first
push_down_group_one_tag_filter_field_first
push_down_group_one_tag_last
push_down_group_all_filter_field_last
push_down_group_one_tag_filter_field_last

windowed_by_time_count # TODO(bnpfeife) broken by flux@05a1065f, OptimizeAggregateWindow
windowed_by_time_sum   # TODO(bnpfeife) broken by flux@05a1065f, OptimizeAggregateWindow
windowed_by_time_mean  # TODO(bnpfeife) broken by flux@05a1065f, OptimizeAggregateWindow
ENDSKIPS
)
  echo "$doc" | sed '/^[[:space:]]*$/d' | sed 's/[[:space:]]*#.*$//' | tr '\n' ',' | sed 's/,$//'
}

run_integration_tests() {
  log "Running integration tests..."
  ./fluxtest -v -p flux.zip -p flux/stdlib --skip "$DB_TESTS"
}

cleanup() {
  rm -f flux.zip fluxtest
}

main() {
  build_test_harness
  download_flux_archive
  run_integration_tests
  cleanup
}
main
