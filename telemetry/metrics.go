package telemetry

import (
	pr "github.com/influxdata/influxdb/prometheus"
)

var telemetryMatcher = pr.NewMatcher().
	Family("influxdb_info").
	Family("influxdb_uptime_seconds").
	Family("influxdb_organizations_total").
	Family("influxdb_buckets_total").
	Family("influxdb_users_total").
	Family("influxdb_tokens_total").
	Family("influxdb_dashboards_total").
	Family("influxdb_scrapers_total").
	Family("influxdb_telegrafs_total").
	Family("http_api_requests_total",
		pr.L("handler", "platform"),
		pr.L("method", "GET"),
		pr.L("path", "/api/v2"),
		pr.L("status", "2XX"),
	)
