package tsm1

import (
	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/index/inmem"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestEngine_ConcurrentShardSnapshots(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Skipping on windows")
	}

	tmpDir, err := ioutil.TempDir("", "shard_test")
	if err != nil {
		t.Fatalf("error creating temporary directory: %s", err.Error())
	}
	defer os.RemoveAll(tmpDir)
	tmpShard := filepath.Join(tmpDir, "shard")
	tmpWal := filepath.Join(tmpDir, "wal")

	sfile := NewSeriesFile(tmpDir)
	defer sfile.Close()

	opts := tsdb.NewEngineOptions()
	opts.Config.WALDir = filepath.Join(tmpDir, "wal")
	opts.InmemIndex = inmem.NewIndex(filepath.Base(tmpDir), sfile)
	opts.SeriesIDSets = seriesIDSets([]*tsdb.SeriesIDSet{})

	sh := tsdb.NewShard(1, tmpShard, tmpWal, sfile, opts)
	if err := sh.Open(); err != nil {
		t.Fatalf("error opening shard: %s", err.Error())
	}
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
	err = sh.WritePoints(points)
	if err != nil {
		t.Fatalf(err.Error())
	}

	engineInterface, err := sh.Engine()
	if err != nil {
		t.Fatalf("error retrieving shard.Engine(): %s", err.Error())
	}

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
			if err = os.RemoveAll(f); err != nil {
				t.Fatalf("Failed to clean up in TestEngine_ConcurrentShardSnapshots: %s", err.Error())
			}
		} else if err == ErrSnapshotInProgress {
			if skipCacheOk {
				t.Fatalf("failing to ignore this error,: %s", err.Error())
			}
		} else if err != nil {
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
func NewSeriesFile(tmpDir string) *tsdb.SeriesFile {
	dir, err := ioutil.TempDir(tmpDir, "tsdb-series-file-")
	if err != nil {
		panic(err)
	}
	f := tsdb.NewSeriesFile(dir)
	f.Logger = logger.New(os.Stdout)
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
