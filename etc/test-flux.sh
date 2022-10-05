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
