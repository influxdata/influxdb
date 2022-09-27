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
# vectorization-related tests
logical_typed_null_vectorized_const
logical_untyped_null_vectorized_const

vec_conditional_bool
vec_conditional_bool_repeat
vec_conditional_time
vec_conditional_time_repeat
vec_conditional_int
vec_conditional_int_repeat
vec_conditional_float
vec_conditional_float_repeat
vec_conditional_uint
vec_conditional_string
vec_conditional_string_repeat
vec_conditional_null_test
vec_conditional_null_consequent
vec_conditional_null_alternate
vec_conditional_null_consequent_alternate
vec_conditional_null_test_consequent_alternate
vec_const_bools
vec_const_with_const
vec_const_with_const_add_const
vec_const_add_member_const
vec_const_with_const_add_const_add_member
vec_const_with_const_add_member_add_const
vec_const_with_member_add_const_add_const
vec_const_kitchen_sink_column_types
vec_equality_time
vec_equality_time_repeat
vec_equality_int
vec_equality_int_repeat
vec_equality_float
vec_equality_float_repeat
vec_equality_uint
vec_equality_string
vec_equality_string_repeat
vec_equality_bool
vec_equality_casts
vec_nested_logical_conditional_repro
vec_nested_logical_conditional_repro2
vec_with_float
vec_with_float_const
vec_with_unary_add
vec_with_unary_sub
vec_with_unary_not
vec_with_unary_exists
vectorize_div_by_zero_int_const
vectorize_div_by_zero_int_const_const

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

# Needs feature flag labelPolymorphism
label_to_string

# Bug https://github.com/influxdata/flux/issues/5232
#   Or, needs feature flag strictNullLogicalOps
logical_typed_null_interp

# https://github.com/influxdata/influxdb/issues/23757
# Flux acceptance tests for group |> first (and last)
push_down_group_one_tag_first
push_down_group_all_filter_field_first
push_down_group_one_tag_filter_field_first
push_down_group_one_tag_last
push_down_group_all_filter_field_last
push_down_group_one_tag_filter_field_last

group_one_tag_last              # broken (fixed in flux@3d6f47ded)
group_all_filter_field_last     # broken (fixed in flux@3d6f47ded)
group_one_tag_filter_field_last # broken (fixed in flux@3d6f47ded)

windowed_by_time_count # TODO(bnpfeife) broken by flux@05a1065f, OptimizeAggregateWindow
windowed_by_time_sum   # TODO(bnpfeife) broken by flux@05a1065f, OptimizeAggregateWindow
windowed_by_time_mean  # TODO(bnpfeife) broken by flux@05a1065f, OptimizeAggregateWindow
ENDSKIPS
)
  echo "$doc" | sed '/^[[:space:]]*$/d' | sed 's/[[:space:]]*#.*$//' | tr '\n' ',' | sed 's/,$//'
}

run_integration_tests() {
  log "Running integration tests..."
  ./fluxtest \
      -v \
      -p flux.zip \
      -p flux/ \
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
