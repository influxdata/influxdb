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

# Many tests targeting 3rd party databases are not yet supported in CI and should be filtered out.
DB_INTEGRATION_WRITE_TESTS=integration_sqlite_write_to,integration_vertica_write_to,integration_mssql_write_to,integration_mysql_write_to,integration_mariadb_write_to,integration_pg_write_to,integration_hdb_write_to
DB_INTEGRATION_READ_TESTS=integration_sqlite_read_from_seed,integration_sqlite_read_from_nonseed,integration_vertica_read_from_seed,integration_vertica_read_from_nonseed,integration_mssql_read_from_seed,integration_mssql_read_from_nonseed,integration_mariadb_read_from_seed,integration_mariadb_read_from_nonseed,integration_mysql_read_from_seed,integration_mysql_read_from_nonseed,integration_pg_read_from_seed,integration_pg_read_from_nonseed,integration_hdb_read_from_seed,integration_hdb_read_from_nonseed
DB_TESTS="${DB_INTEGRATION_WRITE_TESTS},${DB_INTEGRATION_READ_TESTS}"

run_integration_tests() {
  log "Running integration tests..."
  ./fluxtest \
      -v \
      -p flux.zip \
      -p query/ \
      --skip "${DB_TESTS}",group_one_tag_first,group_all_filter_field_first,group_one_tag_filter_field_first,group_one_tag_last,group_all_filter_field_last,group_one_tag_filter_field_last,integration_hdb_injection,integration_pg_injection,integration_mysql_injection,integration_mariadb_injection,integration_mssql_injection
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
