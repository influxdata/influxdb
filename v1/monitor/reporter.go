package monitor

import "github.com/influxdata/influxdb/v2/models"

// Reporter is an interface for gathering internal statistics.
type Reporter interface {
	// Statistics returns the statistics for the reporter,
	// with the given tags merged into the result.
	Statistics(tags map[string]string) []models.Statistic
}
