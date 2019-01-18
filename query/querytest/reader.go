package querytest

import (
	"context"
	"math"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/execute/executetest"
	"github.com/influxdata/influxdb/query/stdlib/influxdata/influxdb"
	"github.com/influxdata/influxdb/storage/reads"
)

func ReaderTestHelper(
	t *testing.T,
	start execute.Time,
	stop execute.Time,
	addTransformations func(d execute.Dataset, c execute.TableBuilderCache, s execute.Source) error,
	data []*Table,
	want []*executetest.Table,
) {
	t.Helper()

	if start > stop {
		t.Fatal("invalid bounds, start > stop")
	}

	for _, tbl := range data {
		if err := tbl.Check(); err != nil {
			t.Fatal(err)
		}
	}

	bounds := execute.Bounds{
		Start: start,
		Stop:  stop,
	}
	now := stop

	store, err := NewStore(data, bounds)
	if err != nil {
		t.Fatal(err)
	}

	id := executetest.RandomDatasetID()
	d := executetest.NewDataset(id)
	c := execute.NewTableBuilderCache(executetest.UnlimitedAllocator)
	c.SetTriggerSpec(flux.DefaultTrigger)
	s := influxdb.NewSource(
		id,
		reads.NewReader(store),
		influxdb.ReadSpec{PointsLimit: math.MaxInt64},
		bounds,
		execute.Window{
			Period: execute.Duration(stop - start),
			Every:  execute.Duration(stop - start),
		},
		now)

	err = addTransformations(d, c, s)
	if err != nil {
		t.Fatal(err)
	}

	s.Run(context.Background())
	got, err := executetest.TablesFromCache(c)
	if err != nil {
		t.Fatal(err)
	}

	executetest.NormalizeTables(got)
	executetest.NormalizeTables(want)

	sort.Sort(executetest.SortedTables(got))
	sort.Sort(executetest.SortedTables(want))

	if !cmp.Equal(want, got) {
		t.Errorf("unexpected tables -want/+got\n%s", cmp.Diff(want, got))
	}
}
