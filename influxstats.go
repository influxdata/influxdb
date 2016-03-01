package influxdb

import (
	"expvar"
	"github.com/influxdata/influxdb/stats"
)

// NewStatistics returns an expvar-based map with the given key.
//
// This function is deprecated  and will be removed.
//
// Use stats.Root.NewBuilder(...).MustBuild().Open().ValuesMap() instead.
//
func NewStatistics(key, name string, tags map[string]string) *expvar.Map {
	return stats.Root.
		NewBuilder(key, name, tags).
		DisableIdleTimer().
		MustBuild().
		Open().
		ValuesMap()
}
