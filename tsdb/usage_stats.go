package tsdb

import (
	"github.com/influxdata/influxdb/models"
	"sync"
)

// Thread-safe (synchronized) container for measurement-level information
// (currently only for disk size)
type usageStats struct {
	// fillStats is the next set of stats, it can be filled but not queried.
	// FinishStats moves fillStats to showStats
	fillStats map[string]map[string]float64
	// showStats is the current set of stats available by calling Statistics .
	showStats map[string]map[string]float64
	mu        sync.Mutex
}

func (s *usageStats) AddDiskSize(database, measurement string, stat float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fillStats == nil {
		s.fillStats = make(map[string]map[string]float64)
	}
	if _, ok := s.fillStats[database]; !ok {
		s.fillStats[database] = make(map[string]float64)
	}
	s.fillStats[database][measurement] += stat
}

func (s *usageStats) FinishedAdding() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.showStats = s.fillStats
	s.fillStats = nil
}

func (s *usageStats) Statistics(tags map[string]string) []models.Statistic {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Add all the series and measurements cardinality estimations.
	statistics := make([]models.Statistic, 0)
	for database, measures := range s.showStats {
		for measure, val := range measures {
			statistics = append(statistics, models.Statistic{
				Name: "usage",
				Tags: models.StatisticTags{"database": database, "measurement": measure}.Merge(tags),
				Values: map[string]interface{}{
					"disksize": val,
				},
			})
		}
	}
	return statistics
}
