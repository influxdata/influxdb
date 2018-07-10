package executetest

import (
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/execute"
)

func ProcessTestHelper(
	t *testing.T,
	data []query.Table,
	want []*Table,
	create func(d execute.Dataset, c execute.TableBuilderCache) execute.Transformation,
) {
	t.Helper()

	d := NewDataset(RandomDatasetID())
	c := execute.NewTableBuilderCache(UnlimitedAllocator)
	c.SetTriggerSpec(execute.DefaultTriggerSpec)

	tx := create(d, c)

	parentID := RandomDatasetID()
	for _, b := range data {
		if err := tx.Process(parentID, b); err != nil {
			t.Fatal(err)
		}
	}

	got, err := TablesFromCache(c)
	if err != nil {
		t.Fatal(err)
	}

	NormalizeTables(got)
	NormalizeTables(want)

	sort.Sort(SortedTables(got))
	sort.Sort(SortedTables(want))

	if !cmp.Equal(want, got, cmpopts.EquateNaNs()) {
		t.Errorf("unexpected tables -want/+got\n%s", cmp.Diff(want, got))
	}
}
