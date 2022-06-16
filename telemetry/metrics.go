package telemetry

import (
	pr "github.com/influxdata/influxdb/v2/prometheus"
)

var telemetryMatcher = pr.NewMatcher().
	/*
	 *   Runtime stats
	 */
	Family("influxdb_info"). // includes version, os, etc.
	Family("influxdb_uptime_seconds").
	/*
	 * Resource Counts
	 */
	Family("influxdb_organizations_total").
	Family("influxdb_buckets_total").
	Family("influxdb_users_total").
	Family("influxdb_tokens_total").
	Family("influxdb_dashboards_total").
	Family("influxdb_scrapers_total").
	Family("influxdb_telegrafs_total").
	Family("influxdb_telegraf_plugins_count").
	Family("influxdb_remotes_total").
	Family("influxdb_replications_total").
	Family("task_scheduler_claims_active"). // Count of currently active tasks
	/*
	 * Count of API requests including success and failure
	 */
	Family("http_api_requests_total").
	/*
	 * Count of writes and queries
	 */
	Family("storage_wal_writes_total").
	Family("query_control_requests_total").
	/*
	 * Query analysis
	 */
	Family("query_control_functions_total").      // Count of functions in queries (e.g. mean, median)
	Family("query_control_all_duration_seconds"). // Total query duration per org.
	/*
	 * Write analysis
	 */
	Family("http_api_request_duration_seconds_bucket",
		pr.L("path", "/api/v2/write"), // Count only the durations of the /write endpoint.
	).
	/*
	 * Storage cardinality
	 */
	Family("storage_tsi_index_series_total").
	/*
	 * Storage disk usage
	 */
	Family("storage_series_file_disk_bytes").    // All families need to be aggregated to
	Family("storage_wal_current_segment_bytes"). // get a true idea of disk usage.
	Family("storage_tsm_files_disk_bytes")
