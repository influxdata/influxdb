package tsdb

import (
	"github.com/influxdata/influxdb/models"
	tassert "github.com/stretchr/testify/assert"
	"sort"
	"testing"
)

func Test_usageStats(t *testing.T) {
	assert := tassert.New(t)
	var testStats usageStats
	tags := map[string]string{
		"host": "host-1",
	}

	// Len and Statistics are safe for uninitialized variables
	assert.Equal([]models.Statistic{}, testStats.Statistics(tags))

	// Statistics are available after some AddDiskSize calls
	testStats.AddDiskSize("telegraf", "cpu", 10)
	testStats.AddDiskSize("telegraf", "diskio", 9.3)
	testStats.AddDiskSize("telegraf", "cpu", 8)
	testStats.AddDiskSize("fitbit", "heartrate", 7)
	testStats.FinishedAdding()
	stats := testStats.Statistics(tags)
	// Sort slices for comparison - no defined sort order
	sort.Slice(stats, func(i, j int) bool {
		if stats[i].Tags["database"] != stats[j].Tags["database"] {
			return stats[i].Tags["database"] < stats[j].Tags["database"]
		}
		return stats[i].Tags["measurement"] < stats[j].Tags["measurement"]
	})
	assert.Equal([]models.Statistic{
		models.Statistic{
			Name:   "usage",
			Tags:   map[string]string{"database": "fitbit", "measurement": "heartrate", "host": "host-1"},
			Values: map[string]interface{}{"disksize": 7.0},
		},
		models.Statistic{
			Name:   "usage",
			Tags:   map[string]string{"database": "telegraf", "measurement": "cpu", "host": "host-1"},
			Values: map[string]interface{}{"disksize": 18.0},
		},
		models.Statistic{
			Name:   "usage",
			Tags:   map[string]string{"database": "telegraf", "measurement": "diskio", "host": "host-1"},
			Values: map[string]interface{}{"disksize": 9.3},
		},
	}, stats)

	// Flip to the next set of statistics - it's empty
	testStats.FinishedAdding()
	assert.Equal([]models.Statistic{}, testStats.Statistics(tags))
}
