package monitor

import (
	"math"

	"github.com/influxdata/influxdb/monitor/diagnostics"
	"github.com/influxdata/influxdb/pkg/limiter"
)

// stats captures statistics
type stats struct {
	comp compactThroughputStats
}

type compactThroughputStats struct {
	limiter limiter.Rate
	burst   int
}

func (s *stats) CompactThroughputUsage() float64 {
	if s.comp.burst == 0 {
		return 0.0
	}
	available := s.comp.limiter.Tokens()
	percentage := ((float64(s.comp.burst) - available) / float64(s.comp.burst)) * 100
	return math.Round(percentage*10) / 10
}

func (s *stats) Diagnostics() (*diagnostics.Diagnostics, error) {
	compactThroughputUsage := s.CompactThroughputUsage()
	d := map[string]interface{}{
		"compact-throughput-usage": compactThroughputUsage,
	}

	return diagnostics.RowFromMap(d), nil
}
