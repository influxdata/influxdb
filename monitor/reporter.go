package monitor

import "github.com/influxdata/influxdb/models"

// Reporter is an interface for gathering internal statistics
type Reporter interface {
	Statistics(tags map[string]string) []models.Statistic
}
