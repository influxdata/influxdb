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

# Other skipped tests
buckets # unbounded
columns # failing with differences
cov # unbounded
covariance # failing with differences
cumulative_sum # failing with differences
cumulative_sum_default # failing with differences
cumulative_sum_noop # failing with differences
difference_columns  # failing with differences
distinct # failing with differences
fill # failing with differences
first # unbounded
group # unbounded
highestAverage # unbounded
highestMax # unbounded
histogram # unbounded
histogram_quantile # failing with differences
histogram_quantile_minvalue # failing with error
join # unbounded
join_missing_on_col # unbounded
join_panic # unbounded
key_values # unbounded
key_values_host_name # unbounded
keys # failing with differences
last # unbounded
lowestAverage # failing with differences
map # unbounded
max # unbounded
min # unbounded
pivot_mean # failing with differences
sample # unbounded
secrets # failing with error
selector_preserve_time # failing with differences
set # failing with differences
shapeData # failing with differences
shapeDataWithFilter # failing with differences
shift # unbounded
shift_negative_duration # unbounded
state_changes_big_any_to_any # unbounded
state_changes_big_info_to_ok # unbounded
state_changes_big_ok_to_info # unbounded
union # unbounded
union_heterogeneous # unbounded
unique # unbounded
window_null # failing with differences
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
