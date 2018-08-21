package tsdb

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxql"
)

func TestShard_MapType(t *testing.T) {
	var sh *TempShard

	setup := func(index string) {
		sh = NewTempShard(index)

		if err := sh.Open(); err != nil {
			t.Fatal(err)
		}

		sh.MustWritePointsString(`
cpu,host=serverA,region=uswest value=100 0
cpu,host=serverA,region=uswest value=50,val2=5  10
cpu,host=serverB,region=uswest value=25  0
mem,host=serverA value=25i 0
mem,host=serverB value=50i,val3=t 10
_reserved,region=uswest value="foo" 0
`)
	}

	for _, index := range RegisteredIndexes() {
		setup(index)
		for _, tt := range []struct {
			measurement string
			field       string
			typ         influxql.DataType
		}{
			{
				measurement: "cpu",
				field:       "value",
				typ:         influxql.Float,
			},
			{
				measurement: "cpu",
				field:       "host",
				typ:         influxql.Tag,
			},
			{
				measurement: "cpu",
				field:       "region",
				typ:         influxql.Tag,
			},
			{
				measurement: "cpu",
				field:       "val2",
				typ:         influxql.Float,
			},
			{
				measurement: "cpu",
				field:       "unknown",
				typ:         influxql.Unknown,
			},
			{
				measurement: "mem",
				field:       "value",
				typ:         influxql.Integer,
			},
			{
				measurement: "mem",
				field:       "val3",
				typ:         influxql.Boolean,
			},
			{
				measurement: "mem",
				field:       "host",
				typ:         influxql.Tag,
			},
			{
				measurement: "unknown",
				field:       "unknown",
				typ:         influxql.Unknown,
			},
			{
				measurement: "_fieldKeys",
				field:       "fieldKey",
				typ:         influxql.String,
			},
			{
				measurement: "_fieldKeys",
				field:       "fieldType",
				typ:         influxql.String,
			},
			{
				measurement: "_fieldKeys",
				field:       "unknown",
				typ:         influxql.Unknown,
			},
			{
				measurement: "_series",
				field:       "key",
				typ:         influxql.String,
			},
			{
				measurement: "_series",
				field:       "unknown",
				typ:         influxql.Unknown,
			},
			{
				measurement: "_tagKeys",
				field:       "tagKey",
				typ:         influxql.String,
			},
			{
				measurement: "_tagKeys",
				field:       "unknown",
				typ:         influxql.Unknown,
			},
			{
				measurement: "_reserved",
				field:       "value",
				typ:         influxql.String,
			},
			{
				measurement: "_reserved",
				field:       "region",
				typ:         influxql.Tag,
			},
		} {
			name := fmt.Sprintf("%s_%s_%s", index, tt.measurement, tt.field)
			t.Run(name, func(t *testing.T) {
				typ, err := sh.mapType(tt.measurement, tt.field)
				if err != nil {
					t.Fatal(err)
				}

				if have, want := typ, tt.typ; have != want {
					t.Errorf("unexpected data type: have=%#v want=%#v", have, want)
				}
			})
		}
		sh.Close()
	}
}

func TestShard_MeasurementsByRegex(t *testing.T) {
	var sh *TempShard
	setup := func(index string) {
		sh = NewTempShard(index)
		if err := sh.Open(); err != nil {
			t.Fatal(err)
		}

		sh.MustWritePointsString(`
cpu,host=serverA,region=uswest value=100 0
cpu,host=serverA,region=uswest value=50,val2=5  10
cpu,host=serverB,region=uswest value=25  0
mem,host=serverA value=25i 0
mem,host=serverB value=50i,val3=t 10
`)
	}

	for _, index := range RegisteredIndexes() {
		setup(index)
		for _, tt := range []struct {
			regex        string
			measurements []string
		}{
			{regex: `cpu`, measurements: []string{"cpu"}},
			{regex: `mem`, measurements: []string{"mem"}},
			{regex: `cpu|mem`, measurements: []string{"cpu", "mem"}},
			{regex: `gpu`, measurements: []string{}},
			{regex: `pu`, measurements: []string{"cpu"}},
			{regex: `p|m`, measurements: []string{"cpu", "mem"}},
		} {
			t.Run(index+"_"+tt.regex, func(t *testing.T) {
				re := regexp.MustCompile(tt.regex)
				measurements, err := sh.MeasurementNamesByRegex(re)
				if err != nil {
					t.Fatal(err)
				}

				mstrings := make([]string, 0, len(measurements))
				for _, name := range measurements {
					mstrings = append(mstrings, string(name))
				}
				sort.Strings(mstrings)
				if diff := cmp.Diff(tt.measurements, mstrings, cmpopts.EquateEmpty()); diff != "" {
					t.Errorf("unexpected measurements:\n%s", diff)
				}
			})
		}
		sh.Close()
	}
}

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
	opt.IndexVersion = index
	opt.Config.WALDir = filepath.Join(dir, "wal")
	if index == InmemIndexName {
		opt.InmemIndex, _ = NewInmemIndex(path.Base(dir), sfile)
	}

	return &TempShard{
		Shard: NewShard(0,
			filepath.Join(dir, "data", "db0", "rp0", "1"),
			filepath.Join(dir, "wal", "db0", "rp0", "1"),
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
