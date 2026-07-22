package coordinator_test

import (
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/influxdata/influxdb/coordinator"
	"github.com/stretchr/testify/require"
)

func TestConfig_Parse(t *testing.T) {
	// Parse configuration.
	var c coordinator.Config
	_, err := toml.Decode(`
write-timeout = "20s"
max-time-range = "72h"
`, &c)
	require.NoError(t, err)

	// Validate configuration.
	require.Equal(t, 20*time.Second, time.Duration(c.WriteTimeout))
	require.Equal(t, 72*time.Hour, time.Duration(c.MaxTimeRange))
}
