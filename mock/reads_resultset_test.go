package mock_test

import (
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/influxdata/influxdb/v2/pkg/data/gen"
	"github.com/influxdata/influxdb/v2/storage/reads"
	"github.com/influxdata/influxdb/v2/tsdb/cursors"
)

func mustNewSpecFromToml(tb testing.TB, toml string) *gen.Spec {
	tb.Helper()

	spec, err := gen.NewSpecFromToml(toml)
	if err != nil {
		panic(err)
	}

	return spec
}

func TestNewResultSetFromSeriesGenerator(t *testing.T) {
	checkResult := func(t *testing.T, rs reads.ResultSet, expData string, expStats cursors.CursorStats) {
		t.Helper()

		var sb strings.Builder
		err := reads.ResultSetToLineProtocol(&sb, rs)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		if got, exp := sb.String(), expData; !cmp.Equal(got, exp) {
			t.Errorf("unexpected value -got/+exp\n%s", cmp.Diff(got, exp))
		}

		if got, exp := rs.Stats(), expStats; !cmp.Equal(got, exp) {
			t.Errorf("unexpected value -got/+exp\n%s", cmp.Diff(got, exp))
		}
	}

	t.Run("float", func(t *testing.T) {
		spec := mustNewSpecFromToml(t, `
[[measurements]]
name = "m0"
sample = 1.0
tags = [
	{ name = "tag0", source = { type = "sequence", start = 0, count = 3 } },
	{ name = "tag1", source = { type = "sequence", start = 0, count = 2 } },
]
fields = [
	{ name = "v0", count = 3, source = 1.0 },
]`)

		sg := gen.NewSeriesGeneratorFromSpec(spec, gen.TimeRange{
			Start: time.Unix(1000, 0),
			End:   time.Unix(2000, 0),
		})
		const expData = `m0,tag0=value0,tag1=value0 v0=1 1000000000000
m0,tag0=value0,tag1=value0 v0=1 1333333000000
m0,tag0=value0,tag1=value0 v0=1 1666666000000
m0,tag0=value0,tag1=value1 v0=1 1000000000000
m0,tag0=value0,tag1=value1 v0=1 1333333000000
m0,tag0=value0,tag1=value1 v0=1 1666666000000
m0,tag0=value1,tag1=value0 v0=1 1000000000000
m0,tag0=value1,tag1=value0 v0=1 1333333000000
m0,tag0=value1,tag1=value0 v0=1 1666666000000
m0,tag0=value1,tag1=value1 v0=1 1000000000000
m0,tag0=value1,tag1=value1 v0=1 1333333000000
m0,tag0=value1,tag1=value1 v0=1 1666666000000
m0,tag0=value2,tag1=value0 v0=1 1000000000000
m0,tag0=value2,tag1=value0 v0=1 1333333000000
m0,tag0=value2,tag1=value0 v0=1 1666666000000
m0,tag0=value2,tag1=value1 v0=1 1000000000000
m0,tag0=value2,tag1=value1 v0=1 1333333000000
m0,tag0=value2,tag1=value1 v0=1 1666666000000
`
		expStats := cursors.CursorStats{ScannedValues: 18, ScannedBytes: 18 * 8}
		checkResult(t, mock.NewResultSetFromSeriesGenerator(sg), expData, expStats)
	})

	t.Run("max", func(t *testing.T) {
		spec := mustNewSpecFromToml(t, `
[[measurements]]
name = "m0"
sample = 1.0
tags = [
	{ name = "tag0", source = { type = "sequence", start = 0, count = 3 } },
	{ name = "tag1", source = { type = "sequence", start = 0, count = 2 } },
]
fields = [
	{ name = "v0", count = 3, source = 1.0 },
]`)

		sg := gen.NewSeriesGeneratorFromSpec(spec, gen.TimeRange{
			Start: time.Unix(1000, 0),
			End:   time.Unix(2000, 0),
		})
		const expData = `m0,tag0=value0,tag1=value0 v0=1 1000000000000
m0,tag0=value0,tag1=value0 v0=1 1333333000000
m0,tag0=value0,tag1=value0 v0=1 1666666000000
m0,tag0=value0,tag1=value1 v0=1 1000000000000
m0,tag0=value0,tag1=value1 v0=1 1333333000000
`
		expStats := cursors.CursorStats{ScannedValues: 5, ScannedBytes: 5 * 8}
		checkResult(t, mock.NewResultSetFromSeriesGenerator(sg, mock.WithGeneratorMaxValues(5)), expData, expStats)
	})

}
