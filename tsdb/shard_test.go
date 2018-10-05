package tsdb_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/influxdata/influxql"
	"github.com/influxdata/platform/models"
	"github.com/influxdata/platform/tsdb"
	_ "github.com/influxdata/platform/tsdb/tsm1"
)

// Shard represents a test wrapper for tsdb.Shard.
type Shard struct {
	*tsdb.Shard
	sfile *SeriesFile
	path  string
}

type Shards []*Shard

// NewShard returns a new instance of Shard with temp paths.
func NewShard() *Shard {
	return NewShards(1)[0]
}

// MustNewOpenShard creates and opens a shard with the provided index.
func MustNewOpenShard() *Shard {
	sh := NewShard()
	if err := sh.Open(); err != nil {
		panic(err)
	}
	return sh
}

// Close closes the shard and removes all underlying data.
func (sh *Shard) Close() error {
	// Will remove temp series file data.
	if err := sh.sfile.Close(); err != nil {
		return err
	}

	defer os.RemoveAll(sh.path)
	return sh.Shard.Close()
}

// NewShards create several shards all sharing the same
func NewShards(n int) Shards {
	// Create temporary path for data and WAL.
	dir, err := ioutil.TempDir("", "influxdb-tsdb-")
	if err != nil {
		panic(err)
	}

	sfile := MustOpenSeriesFile()

	var shards []*Shard
	var idSets []*tsdb.SeriesIDSet
	for i := 0; i < n; i++ {
		idSets = append(idSets, tsdb.NewSeriesIDSet())
	}

	for i := 0; i < n; i++ {
		// Build engine options.
		opt := tsdb.NewEngineOptions()

		// Initialise series id sets. Need to do this as it's normally done at the
		// store level.
		opt.SeriesIDSets = seriesIDSets(idSets)

		sh := &Shard{
			Shard: tsdb.NewShard(uint64(i),
				filepath.Join(dir, "data", "db0", "rp0", fmt.Sprint(i)),
				sfile.SeriesFile,
				opt,
			),
			sfile: sfile,
			path:  dir,
		}

		shards = append(shards, sh)
	}
	return Shards(shards)
}

// Open opens all the underlying shards.
func (a Shards) Open() error {
	for _, sh := range a {
		if err := sh.Open(); err != nil {
			return err
		}
	}
	return nil
}

// MustOpen opens all the shards, panicking if an error is encountered.
func (a Shards) MustOpen() {
	if err := a.Open(); err != nil {
		panic(err)
	}
}

// Close closes all shards and removes all underlying data.
func (a Shards) Close() error {
	if len(a) == 1 {
		return a[0].Close()
	}

	// Will remove temp series file data.
	if err := a[0].sfile.Close(); err != nil {
		return err
	}

	defer os.RemoveAll(a[0].path)
	for _, sh := range a {
		if err := sh.Shard.Close(); err != nil {
			return err
		}
	}
	return nil
}

// MustWritePointsString parses the line protocol (with second precision) and
// inserts the resulting points into the shard. Panic on error.
func (sh *Shard) MustWritePointsString(s string) {
	a, err := models.ParsePointsWithPrecision([]byte(strings.TrimSpace(s)), time.Time{}, "s")
	if err != nil {
		panic(err)
	}

	if err := sh.WritePoints(a); err != nil {
		panic(err)
	}
}

func MustTempDir() (string, func()) {
	dir, err := ioutil.TempDir("", "shard-test")
	if err != nil {
		panic(fmt.Sprintf("failed to create temp dir: %v", err))
	}
	return dir, func() { os.RemoveAll(dir) }
}

type seriesIterator struct {
	keys [][]byte
}

type series struct {
	name    []byte
	tags    models.Tags
	deleted bool
}

func (s series) Name() []byte        { return s.name }
func (s series) Tags() models.Tags   { return s.tags }
func (s series) Deleted() bool       { return s.deleted }
func (s series) Expr() influxql.Expr { return nil }

func (itr *seriesIterator) Close() error { return nil }

func (itr *seriesIterator) Next() (tsdb.SeriesElem, error) {
	if len(itr.keys) == 0 {
		return nil, nil
	}
	name, tags := models.ParseKeyBytes(itr.keys[0])
	s := series{name: name, tags: tags}
	itr.keys = itr.keys[1:]
	return s, nil
}

type seriesIDSets []*tsdb.SeriesIDSet

func (a seriesIDSets) ForEach(f func(ids *tsdb.SeriesIDSet)) error {
	for _, v := range a {
		f(v)
	}
	return nil
}
