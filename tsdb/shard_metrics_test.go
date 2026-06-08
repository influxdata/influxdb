package tsdb_test

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/influxdata/influxdb/v2/kit/prom/promtest"
	"github.com/influxdata/influxdb/v2/tsdb"
	"github.com/influxdata/influxdb/v2/tsdb/engine/tsm1"
)

// metricsRegistry returns a registry holding every per-shard collector family
// (shard, cache, WAL, file store, and compaction).
func metricsRegistry(t *testing.T) *prometheus.Registry {
	t.Helper()
	reg := prometheus.NewRegistry()
	reg.MustRegister(tsdb.ShardCollectors()...)
	// PrometheusCollectors includes the cache, WAL, file store, and compaction
	// families.
	reg.MustRegister(tsm1.PrometheusCollectors()...)
	return reg
}

// TestStore_DeleteShard_RemovesMetrics verifies that permanently removing a
// shard deletes its child series from every metric family.
func TestStore_DeleteShard_RemovesMetrics(t *testing.T) {
	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) {
			s := MustOpenStore(t, index)
			defer s.CloseStore(t, index)

			require.NoError(t, s.CreateShard(context.Background(), "db0", "rp0", 1, true))
			sh := s.Shard(1)
			require.NotNil(t, sh)
			s.MustWriteToShardString(1, "cpu,server=a v=1")

			reg := metricsRegistry(t)
			path := sh.Path()

			before := promtest.CountMetricsByLabel(t, reg, "path", path)
			// These three families register their children at engine open, so
			// they must be present before deletion.
			require.Contains(t, before, "storage_shard_series")
			require.Contains(t, before, "storage_cache_inuse_bytes")
			require.Contains(t, before, "storage_tsm_files_total")

			require.NoError(t, s.DeleteShard(1))

			after := promtest.CountMetricsByLabel(t, reg, "path", path)
			require.Empty(t, after,
				"expected all metric families for the deleted shard to be removed, still present: %v", after)
		})
	}
}

// TestShard_CloseAndRemoveMetrics verifies that permanent removal closes the
// shard before deleting its metric series. Closing first stops the engine's
// background compaction goroutines, which re-create their child series via
// With() on every tick; removing only after they have stopped is what prevents
// a stray tick from resurrecting a deleted series.
func TestShard_CloseAndRemoveMetrics(t *testing.T) {
	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) {
			s := MustOpenStore(t, index)
			defer s.CloseStore(t, index)

			require.NoError(t, s.CreateShard(context.Background(), "db0", "rp0", 1, true))
			sh := s.Shard(1)
			require.NotNil(t, sh)
			s.MustWriteToShardString(1, "cpu,server=a v=1")

			reg := metricsRegistry(t)
			path := sh.Path()
			require.NotEmpty(t, promtest.CountMetricsByLabel(t, reg, "path", path))

			require.NoError(t, sh.CloseAndRemoveMetrics())

			// The engine must be closed once removal has run, so its compaction
			// goroutines have stopped and cannot resurrect a removed series.
			_, err := sh.Engine()
			require.Error(t, err, "engine should be closed after CloseAndRemoveMetrics")

			require.Empty(t, promtest.CountMetricsByLabel(t, reg, "path", path),
				"all metric families for the removed shard must be deleted")
		})
	}
}

// TestStore_ReopenShard_KeepsMetrics verifies that a transient close/reopen
// (which reuses the same Shard object and labels) does NOT remove the shard's
// metric series.
func TestStore_ReopenShard_KeepsMetrics(t *testing.T) {
	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) {
			s := MustOpenStore(t, index)
			defer s.CloseStore(t, index)

			require.NoError(t, s.CreateShard(context.Background(), "db0", "rp0", 1, true))
			sh := s.Shard(1)
			require.NotNil(t, sh)
			s.MustWriteToShardString(1, "cpu,server=a v=1")

			reg := metricsRegistry(t)
			path := sh.Path()
			require.NotEmpty(t, promtest.CountMetricsByLabel(t, reg, "path", path))

			require.NoError(t, s.ReopenShard(context.Background(), 1, false))

			after := promtest.CountMetricsByLabel(t, reg, "path", path)
			require.Contains(t, after, "storage_shard_series",
				"transient reopen must not remove the shard's metric series")
		})
	}
}
