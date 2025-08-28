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

// CompactThroughputUsage calculates the percentage of burst capacity currently consumed by compaction.
//
// available = current tokens in the rate limiter bucket (can be negative when in debt)
// burst = maximum tokens the bucket can hold
// usage percentage = ((burst - available) / burst) * 100
func (s *stats) CompactThroughputUsage() float64 {
	if s.comp.burst == 0 {
		return 0.0
	}
	available := s.comp.limiter.Tokens()

	// Clamp available tokens
	if available < 0 {
		available = 0
	} else if available > float64(s.comp.burst) {
		available = float64(s.comp.burst)
	}

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
