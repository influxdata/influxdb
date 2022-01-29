package tsm1

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/tsdb"
	"github.com/stretchr/testify/require"
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
