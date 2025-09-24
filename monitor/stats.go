package monitor

import (
	"math"

	"github.com/influxdata/influxdb/monitor/diagnostics"
	"github.com/influxdata/influxdb/pkg/limiter"
	"golang.org/x/time/rate"
)

// stats captures statistics
type stats struct {
	comp compactThroughputStats
}

type compactThroughputStats struct {
	limiter limiter.Rate
}

// CompactThroughputUsage calculates the percentage of burst capacity currently consumed by compaction.
func (s *stats) CompactThroughputUsage() float64 {
	percentage := 100 * (1 - rate.Limit(s.comp.limiter.Tokens())/s.comp.limiter.Limit())
	return float64(percentage)
}

func (s *stats) Diagnostics() (*diagnostics.Diagnostics, error) {
	compactThroughputUsage := s.CompactThroughputUsage()
	compactThroughputUsageTrunc := math.Round(compactThroughputUsage*100.0) / 100.0
	d := map[string]interface{}{
		"compact-throughput-usage-percentage": compactThroughputUsageTrunc,
	}

	return diagnostics.RowFromMap(d), nil
}
