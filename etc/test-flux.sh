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
# Integration write tests
integration_mqtt_pub
integration_sqlite_write_to
integration_vertica_write_to
integration_mssql_write_to
integration_mysql_write_to
integration_mariadb_write_to
integration_pg_write_to
integration_hdb_write_to

# Integration read tests
integration_sqlite_read_from_seed
integration_sqlite_read_from_nonseed
integration_vertica_read_from_seed
integration_vertica_read_from_nonseed
integration_mssql_read_from_seed
integration_mssql_read_from_nonseed
integration_mariadb_read_from_seed
integration_mariadb_read_from_nonseed
integration_mysql_read_from_seed
integration_mysql_read_from_nonseed
integration_pg_read_from_seed
integration_pg_read_from_nonseed
integration_hdb_read_from_seed
integration_hdb_read_from_nonseed

# Integration injection tests
integration_sqlite_injection
integration_hdb_injection
integration_pg_injection
integration_mysql_injection
integration_mariadb_injection
integration_mssql_injection

# Tests skipped because a feature flag must be enabled
# the flag is: removeRedundantSortNodes
remove_sort
remove_sort_more_columns
remove_sort_aggregate
remove_sort_selector
remove_sort_filter_range
remove_sort_aggregate_window
remove_sort_join

vec_with_float_typed_null
vec_with_float_untyped_null

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
# Needs feature flag labelPolymorphism
label_to_string


ENDSKIPS
)
  echo "$doc" | sed '/^[[:space:]]*$/d' | sed 's/[[:space:]]*#.*$//' | tr '\n' ',' | sed 's/,$//'
}

run_integration_tests() {
  log "Running integration tests..."
  ./fluxtest \
      -v \
      -p flux.zip \
      -p query/ \
      --skip "$(skipped_tests)"
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
