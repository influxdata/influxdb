package storage_test

import (
	"testing"

	"github.com/influxdata/influxdb/v2/storage"
	"github.com/influxdata/influxdb/v2/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewEngine_RejectsInvalidConfig confirms that the storage config is
// validated before an engine exists. The launcher parses the adaptive cache
// sizing options one at a time and cannot see that one half of a pair is
// missing; this is the check that turns such a config into a failed startup
// instead of a server running with adaptive sizing quietly off. The rules
// themselves are covered where they live, in the tsdb config tests.
func TestNewEngine_RejectsInvalidConfig(t *testing.T) {
	t.Parallel()

	c := storage.NewConfig()
	c.Data.SeriesIDSetCacheMaxSize = 1000

	e, err := storage.NewEngine(t.TempDir(), c)
	require.ErrorIs(t, err, tsdb.ErrAdaptiveCacheSizingPairing)
	assert.Nil(t, e, "a rejected config must not yield an engine to clean up")
}
