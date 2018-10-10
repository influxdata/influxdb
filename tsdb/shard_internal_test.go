package tsdb

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/influxdata/platform/logger"
	"github.com/influxdata/platform/models"
)

// TempShard represents a test wrapper for Shard that uses temporary
// filesystem paths.
type TempShard struct {
	*Shard
	path  string
	sfile *SeriesFile
}

// NewTempShard returns a new instance of TempShard with temp paths.
func NewTempShard(index string) *TempShard {
	// Create temporary path for data and WAL.
	dir, err := ioutil.TempDir("", "influxdb-tsdb-")
	if err != nil {
		panic(err)
	}

	// Create series file.
	sfile := NewSeriesFile(filepath.Join(dir, "db0", SeriesFileDirectory))
	sfile.Logger = logger.New(os.Stdout)
	if err := sfile.Open(); err != nil {
		panic(err)
	}

	// Build engine options.
	opt := NewEngineOptions()

	return &TempShard{
		Shard: NewShard(0,
			filepath.Join(dir, "data", "db0", "rp0", "1"),
			sfile,
			opt,
		),
		sfile: sfile,
		path:  dir,
	}
}

// Close closes the shard and removes all underlying data.
func (sh *TempShard) Close() error {
	defer os.RemoveAll(sh.path)
	sh.sfile.Close()
	return sh.Shard.Close()
}

// MustWritePointsString parses the line protocol (with second precision) and
// inserts the resulting points into the shard. Panic on error.
func (sh *TempShard) MustWritePointsString(s string) {
	a, err := models.ParsePointsWithPrecision([]byte(strings.TrimSpace(s)), time.Time{}, "s")
	if err != nil {
		panic(err)
	}

	if err := sh.WritePoints(a); err != nil {
		panic(err)
	}
}
