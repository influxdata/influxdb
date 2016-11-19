package monitor

import "github.com/influxdata/influxdb/models"

// Reporter ...
type Reporter interface {
	Statistics(tags map[string]string) []models.Statistic
}
