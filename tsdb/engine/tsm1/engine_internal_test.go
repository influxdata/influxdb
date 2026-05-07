package tsm1

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/pkg/limiter"
	"github.com/influxdata/influxdb/v2/tsdb"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

func TestEngine_ConcurrentShardSnapshots(t *testing.T) {
	tmpDir := t.TempDir()

	tmpShard := filepath.Join(tmpDir, "shard")
	tmpWal := filepath.Join(tmpDir, "wal")

	sfile := NewSeriesFile(t, tmpDir)
	defer sfile.Close()

	opts := tsdb.NewEngineOptions()
	opts.Config.WALDir = filepath.Join(tmpDir, "wal")
	opts.SeriesIDSets = seriesIDSets([]*tsdb.SeriesIDSet{})

	sh := tsdb.NewShard(1, tmpShard, tmpWal, sfile, opts)
	require.NoError(t, sh.Open(context.Background()), "error opening shard")
	defer sh.Close()

	points := make([]models.Point, 0, 10000)
	for i := 0; i < cap(points); i++ {
		points = append(points, models.MustNewPoint(
			"cpu",
			models.NewTags(map[string]string{"host": "server"}),
			map[string]interface{}{"value": 1.0},
			time.Unix(int64(i), 0),
		))
	}
	err := sh.WritePoints(context.Background(), points)
	require.NoError(t, err)

	engineInterface, err := sh.Engine()
	require.NoError(t, err, "error retrieving shard engine")

	// Get the struct underlying the interface. Not a recommended practice.
	realEngineStruct, ok := (engineInterface).(*Engine)
	if !ok {
		t.Log("Engine type does not permit simulating Cache race conditions")
		return
	}
	// fake a race condition in snapshotting the cache.
	realEngineStruct.Cache.snapshotting = true
	defer func() {
		realEngineStruct.Cache.snapshotting = false
	}()

	snapshotFunc := func(skipCacheOk bool) {
		if f, err := sh.CreateSnapshot(skipCacheOk); err == nil {
			require.NoError(t, os.RemoveAll(f), "error cleaning up TestEngine_ConcurrentShardSnapshots")
		} else if err == ErrSnapshotInProgress {
			if skipCacheOk {
				t.Fatalf("failing to ignore this error,: %s", err.Error())
			}
		} else {
			t.Fatalf("error creating shard snapshot: %s", err.Error())
		}
	}

	// Permit skipping cache in the snapshot
	snapshotFunc(true)
	// do not permit skipping the cache in the snapshot
	snapshotFunc(false)
	realEngineStruct.Cache.snapshotting = false
}

// NewSeriesFile returns a new instance of SeriesFile with a temporary file path.
func NewSeriesFile(tb testing.TB, tmpDir string) *tsdb.SeriesFile {
	tb.Helper()

	dir := tb.TempDir()
	f := tsdb.NewSeriesFile(dir)
	f.Logger = zaptest.NewLogger(tb)
	if err := f.Open(); err != nil {
		panic(err)
	}
	return f
}

type seriesIDSets []*tsdb.SeriesIDSet

func (a seriesIDSets) ForEach(f func(ids *tsdb.SeriesIDSet)) error {
	for _, v := range a {
		f(v)
	}
	return nil
}

// TestCompactLoPriorityLevel_ReleasesActiveCount verifies that
// compactLoPriorityLevel returns the activeCompactions level counter to its
// pre-call value after the spawned goroutine completes.
func TestCompactLoPriorityLevel_ReleasesActiveCount(t *testing.T) {
	e := newCompactionTestEngine()

	var wg sync.WaitGroup
	started := e.compactLoPriorityLevel(CompactionGroup{}, 3, true, &wg)
	require.True(t, started, "compactLoPriorityLevel should have started a goroutine")
	wg.Wait()

	require.Equal(t, int64(0), atomic.LoadInt64(&e.activeCompactions.l3),
		"activeCompactions.l3 should return to 0 after the compaction goroutine exits")
}

// TestCompactLoPriorityLevel_LeavesSchedulerRunnable verifies that after a
// compactLoPriorityLevel goroutine completes, Scheduler.next() can still
// pick work for any level whose queue is non-empty.
func TestCompactLoPriorityLevel_LeavesSchedulerRunnable(t *testing.T) {
	const maxConcurrency = 2
	e := newCompactionTestEngine()
	e.compactionLimiter = limiter.NewFixed(maxConcurrency)
	e.Scheduler = newScheduler(e.activeCompactions, maxConcurrency)

	for lvl := 1; lvl <= TotalCompactionLevels; lvl++ {
		e.Scheduler.SetDepth(lvl, 10)
	}

	var wg sync.WaitGroup
	require.True(t, e.compactLoPriorityLevel(CompactionGroup{}, 3, true, &wg))
	wg.Wait()

	level, runnable := e.Scheduler.next()
	require.True(t, runnable,
		"Scheduler.next() should remain runnable after a compaction goroutine exits")
	require.Equal(t, 1, level,
		"Scheduler.next() should pick level 1 when all queues are equally deep and no compactions are active")
}

// TestCompactHiPriorityLevel_ReleasesActiveCount verifies the same invariant
// as TestCompactLoPriorityLevel_ReleasesActiveCount for the hi-priority path.
func TestCompactHiPriorityLevel_ReleasesActiveCount(t *testing.T) {
	e := newCompactionTestEngine()

	var wg sync.WaitGroup
	started := e.compactHiPriorityLevel(CompactionGroup{}, 1, false, &wg)
	require.True(t, started)
	wg.Wait()

	require.Equal(t, int64(0), atomic.LoadInt64(&e.activeCompactions.l1))
}

// TestCompactFull_ReleasesActiveCount verifies that compactFull returns the
// activeCompactions.full counter to its pre-call value after the spawned
// goroutine completes.
func TestCompactFull_ReleasesActiveCount(t *testing.T) {
	e := newCompactionTestEngine()

	var wg sync.WaitGroup
	started := e.compactFull(CompactionGroup{}, &wg)
	require.True(t, started)
	wg.Wait()

	require.Equal(t, int64(0), atomic.LoadInt64(&e.activeCompactions.full),
		"activeCompactions.full should return to 0 after the compaction goroutine exits")
}

// TestCompactOptimize_ReleasesActiveCount verifies that compactOptimize returns
// the activeCompactions.optimize counter to its pre-call value after the
// spawned goroutine completes.
func TestCompactOptimize_ReleasesActiveCount(t *testing.T) {
	e := newCompactionTestEngine()

	var wg sync.WaitGroup
	require.NoError(t, e.compactOptimize(CompactionGroup{}, tsdb.DefaultMaxPointsPerBlock, &wg))
	wg.Wait()

	require.Equal(t, int64(0), atomic.LoadInt64(&e.activeCompactions.optimize),
		"activeCompactions.optimize should return to 0 after the compaction goroutine exits")
}

// newCompactionTestEngine builds a minimal *Engine sufficient for invoking the
// per-level compaction kickoff helpers. The Compactor is left in its default
// disabled state so CompactFast/CompactFull return errCompactionsDisabled
// immediately, letting the spawned goroutine exit quickly without needing
// real TSM files.
func newCompactionTestEngine() *Engine {
	activeCompactions := &compactionCounter{}
	return &Engine{
		id:                         1,
		logger:                     zap.NewNop(),
		Compactor:                  NewCompactor(),
		FileStore:                  &FileStore{},
		CompactionPlan:             &DefaultPlanner{filesInUse: map[string]struct{}{}},
		activeCompactions:          activeCompactions,
		Stats:                      newEngineMetrics(tsdb.EngineTags{}),
		compactionLimiter:          limiter.NewFixed(2),
		optimizedCompactionLimiter: limiter.NewFixed(2),
	}
}
