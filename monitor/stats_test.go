package monitor_test

import (
	"testing"

	"github.com/influxdata/influxdb/monitor"
	"github.com/influxdata/influxdb/pkg/limiter"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/stretchr/testify/require"
)

func TestDiagnostics_Stats(t *testing.T) {
	s := monitor.New(nil, monitor.Config{}, &tsdb.Config{})
	compactLimiter := limiter.NewRate(100, 100)

	s.WithLimiter(compactLimiter)

	require.NoError(t, s.Open(), "opening monitor")
	defer func() {
		require.NoError(t, s.Close(), "closing monitor")
	}()

	d, err := s.Diagnostics()
	require.NoError(t, err, "getting diagnostics")

	diags, ok := d["stats"]
	require.True(t, ok, "expected stats diagnostic client to be registered")

	got, exp := diags.Columns, []string{"compact-throughput-usage"}
	require.Equal(t, exp, got)
}
