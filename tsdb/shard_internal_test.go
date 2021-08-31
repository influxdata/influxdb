package tsdb

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxql"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func TestShard_MapType(t *testing.T) {
	var sh *TempShard

	setup := func(index string) {
		sh = NewTempShard(t, index)

		if err := sh.Open(context.Background()); err != nil {
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
		sh = NewTempShard(t, index)
		if err := sh.Open(context.Background()); err != nil {
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

func TestShard_MeasurementOptimization(t *testing.T) {
	t.Parallel()

	cases := []struct {
		expr  influxql.Expr
		name  string
		ok    bool
		names [][]byte
	}{
		{
			expr:  influxql.MustParseExpr(`_name = 'm0'`),
			name:  "single measurement",
			ok:    true,
			names: [][]byte{[]byte("m0")},
		},
		{
			expr:  influxql.MustParseExpr(`_something = 'f' AND _name = 'm0'`),
			name:  "single measurement with AND",
			ok:    true,
			names: [][]byte{[]byte("m0")},
		},
		{
			expr:  influxql.MustParseExpr(`_something = 'f' AND (a =~ /x0/ AND _name = 'm0')`),
			name:  "single measurement with multiple AND",
			ok:    true,
			names: [][]byte{[]byte("m0")},
		},
		{
			expr:  influxql.MustParseExpr(`_name = 'm0' OR _name = 'm1' OR _name = 'm2'`),
			name:  "multiple measurements alone",
			ok:    true,
			names: [][]byte{[]byte("m0"), []byte("m1"), []byte("m2")},
		},
		{
			expr:  influxql.MustParseExpr(`(_name = 'm0' OR _name = 'm1' OR _name = 'm2') AND (_field = 'foo' OR _field = 'bar' OR _field = 'qux')`),
			name:  "multiple measurements combined",
			ok:    true,
			names: [][]byte{[]byte("m0"), []byte("m1"), []byte("m2")},
		},
		{
			expr:  influxql.MustParseExpr(`(_name = 'm0' OR (_name = 'm1' OR _name = 'm2')) AND tag1 != 'foo'`),
			name:  "parens in expression",
			ok:    true,
			names: [][]byte{[]byte("m0"), []byte("m1"), []byte("m2")},
		},
		{
			expr:  influxql.MustParseExpr(`(tag1 != 'foo' OR tag2 = 'bar') AND (_name = 'm0' OR _name = 'm1' OR _name = 'm2') AND (_field = 'val1' OR _field = 'val2')`),
			name:  "multiple AND",
			ok:    true,
			names: [][]byte{[]byte("m0"), []byte("m1"), []byte("m2")},
		},
		{
			expr:  influxql.MustParseExpr(`(_name = 'm0' OR _name = 'm1' OR _name = 'm2') AND (tag1 != 'foo' OR _name != 'm1')`),
			name:  "measurements on in multiple groups, only one valid group",
			ok:    true,
			names: [][]byte{[]byte("m0"), []byte("m1"), []byte("m2")},
		},
		{
			expr:  influxql.MustParseExpr(`_name = 'm0' OR tag1 != 'foo'`),
			name:  "single measurement with OR",
			ok:    false,
			names: nil,
		},
		{
			expr:  influxql.MustParseExpr(`_name = 'm0' OR true`),
			name:  "measurement with OR boolean literal",
			ok:    false,
			names: nil,
		},
		{
			expr:  influxql.MustParseExpr(`_name != 'm0' AND tag1 != 'foo'`),
			name:  "single measurement with non-equal",
			ok:    false,
			names: nil,
		},
		{
			expr:  influxql.MustParseExpr(`(_name = 'm0' OR _name != 'm1' OR _name = 'm2') AND (_field = 'foo' OR _field = 'bar' OR _field = 'qux')`),
			name:  "multiple measurements with non-equal",
			ok:    false,
			names: nil,
		},
		{
			expr:  influxql.MustParseExpr(`tag1 = 'foo' AND tag2 = 'bar'`),
			name:  "no measurements - multiple tags",
			ok:    false,
			names: nil,
		},
		{
			expr:  influxql.MustParseExpr(`_field = 'foo'`),
			name:  "no measurements - single field",
			ok:    false,
			names: nil,
		},
		{
			expr:  influxql.MustParseExpr(`(_name = 'm0' OR _name = 'm1' AND _name = 'm2') AND tag1 != 'foo'`),
			name:  "measurements with AND",
			ok:    false,
			names: nil,
		},
		{
			expr:  influxql.MustParseExpr(`(_name = 'm0' OR _name = 'm1' OR _name = 'm2') OR (tag1 != 'foo' OR _name = 'm1')`),
			name:  "top level is not AND",
			ok:    false,
			names: nil,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			names, ok := measurementOptimization(tc.expr, measurementKey)
			require.Equal(t, tc.names, names)
			require.Equal(t, tc.ok, ok)
		})
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
func NewTempShard(tb testing.TB, index string) *TempShard {
	tb.Helper()

	// Create temporary path for data and WAL.
	dir, err := ioutil.TempDir("", "influxdb-tsdb-")
	if err != nil {
		panic(err)
	}

	// Create series file.
	sfile := NewSeriesFile(filepath.Join(dir, "db0", SeriesFileDirectory))
	sfile.Logger = zaptest.NewLogger(tb)
	if err := sfile.Open(); err != nil {
		panic(err)
	}

	// Build engine options.
	opt := NewEngineOptions()
	opt.IndexVersion = index
	opt.Config.WALDir = filepath.Join(dir, "wal")

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

	if err := sh.WritePoints(context.Background(), a); err != nil {
		panic(err)
	}
}
