package telemetry

import (
	"context"

	platform "github.com/influxdata/influxdb"
	pr "github.com/influxdata/influxdb/prometheus"
	"github.com/prometheus/client_golang/prometheus"
)

var _ platform.UsageService = (*UsageService)(nil)

var defaultMatcher = pr.NewMatcher().
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

// UsageService filters prometheus metrics for those needed in the usage service.
type UsageService struct {
	pr.Filter
}

// NewUsageService filters the prometheus gatherer to only return metrics
// about the usage statistics.
func NewUsageService(g prometheus.Gatherer) *UsageService {
	return &UsageService{
		Filter: pr.Filter{
			Gatherer: g,
			Matcher:  defaultMatcher,
		},
	}
}

// GetUsage returns usage metrics filtered out of the prometheus metrics.
func (s *UsageService) GetUsage(ctx context.Context, filter platform.UsageFilter) (map[platform.UsageMetric]*platform.Usage, error) {
	return nil, nil
}
