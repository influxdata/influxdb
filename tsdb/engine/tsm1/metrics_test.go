package tsm1

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/influxdata/influxdb/v2/kit/prom/promtest"
	"github.com/influxdata/influxdb/v2/tsdb"
)

// TestTSM1Metrics_remove verifies that each tsm1 metric family's remove()
// deletes that shard's child series from its global vectors. It covers the
// compaction family specifically, whose curried per-level children must be
// removed from the underlying global vectors via DeletePartialMatch.
func TestTSM1Metrics_remove(t *testing.T) {
	// A unique path isolates this test's series from those left in the
	// process-global vectors by other engines/tests.
	const uniquePath = "tsm1-metrics-remove-test-path"
	tags := tsdb.EngineTags{
		Path:          uniquePath,
		WalPath:       uniquePath + "-wal",
		Id:            "987654",
		Bucket:        "bkt",
		EngineVersion: "tsm1",
	}

	cm := newCacheMetrics(tags)
	wm := newWALMetrics(tags)
	fm := newFileStoreMetrics(tags)
	em := newEngineMetrics(tags)

	// The compaction children are created lazily per level, so observe a few
	// distinct levels to populate them.
	em.Active.With(labelForLevel(1)).Set(1)
	em.Queued.With(labelForLevel(2)).Set(1)
	em.Failed.With(labelForLevel(3)).Inc()
	em.Duration.With(labelForLevel(1)).Observe(1)

	reg := prometheus.NewRegistry()
	// PrometheusCollectors includes the cache, WAL, file store, and compaction
	// families.
	reg.MustRegister(PrometheusCollectors()...)

	require.NotEmpty(t, promtest.CountMetricsByLabel(t, reg, "path", uniquePath),
		"expected child series for the shard before removal")

	cm.remove()
	wm.remove()
	fm.remove()
	em.remove()

	require.Empty(t, promtest.CountMetricsByLabel(t, reg, "path", uniquePath),
		"expected all tsm1 child series (including every compaction level) to be removed")
}

// TestCompactionMetrics_resurrectsAfterRemove is a regression guard for the
// ordering of metric removal relative to engine close. The compaction update
// path resolves each child via With() on every call (e.g. PlanCompactions,
// compactCache), so a compaction tick that lands after remove() re-creates the
// deleted series. This is why an engine's metrics must be removed only after
// Close has stopped its compaction goroutines: removing while they still run
// would leak the resurrected series. The test pins that hazard so a change that
// reintroduces remove-before-close is caught.
func TestCompactionMetrics_resurrectsAfterRemove(t *testing.T) {
	const uniquePath = "tsm1-compaction-resurrect-test-path"
	tags := tsdb.EngineTags{
		Path:          uniquePath,
		WalPath:       uniquePath + "-wal",
		Id:            "123456",
		Bucket:        "bkt",
		EngineVersion: "tsm1",
	}
	em := newEngineMetrics(tags)

	reg := prometheus.NewRegistry()
	reg.MustRegister(PrometheusCollectors()...)

	// Populate a compaction child the way the compaction loop does.
	em.Queued.With(labelForLevel(1)).Set(1)
	require.NotEmpty(t, promtest.CountMetricsByLabel(t, reg, "path", uniquePath),
		"expected the compaction child series before removal")

	em.remove()
	require.Empty(t, promtest.CountMetricsByLabel(t, reg, "path", uniquePath),
		"remove() should delete every compaction child for the shard")

	// A compaction-loop tick after remove() resurrects the series, because the
	// update path goes through With(). The engine must therefore be closed (its
	// goroutines stopped) before remove() is called.
	em.Queued.With(labelForLevel(1)).Set(1)
	require.NotEmpty(t, promtest.CountMetricsByLabel(t, reg, "path", uniquePath),
		"a post-remove update resurrects the compaction series; removal must happen after the engine is closed")

	// Leave no series behind in the process-global vectors.
	em.remove()
}
