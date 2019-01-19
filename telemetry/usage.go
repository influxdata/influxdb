package prometheus

import (
	"context"

	platform "github.com/influxdata/influxdb"
	"github.com/prometheus/client_golang/prometheus"
)

var _ platform.UsageService = (*UsageService)(nil)

var defaultMatcher = NewMatcher().
	Family("influxdb_info").
	Family("influxdb_uptime_seconds").
	Family("http_api_requests_total",
		L("handler", "platform"),
		L("method", "GET"),
		L("path", "/api/v2"),
		L("status", "2XX"),
	)

// UsageService filters prometheus metrics for those needed in the usage service.
type UsageService struct {
	Filter
}

// NewUsageService filters the prometheus gatherer to only return metrics
// about the usage statistics.
func NewUsageService(g prometheus.Gatherer) *UsageService {
	return &UsageService{
		Filter: Filter{
			Gatherer: g,
			Matcher:  defaultMatcher,
		},
	}
}

// GetUsage returns usage metrics filtered out of the prometheus metrics.
func (s *UsageService) GetUsage(ctx context.Context, filter platform.UsageFilter) (map[platform.UsageMetric]*platform.Usage, error) {
	return nil, nil
}
