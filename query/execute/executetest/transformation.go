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
	data []query.Block,
	want []*Block,
	create func(d execute.Dataset, c execute.BlockBuilderCache) execute.Transformation,
) {
	t.Helper()

	d := NewDataset(RandomDatasetID())
	c := execute.NewBlockBuilderCache(UnlimitedAllocator)
	c.SetTriggerSpec(execute.DefaultTriggerSpec)

	tx := create(d, c)

	parentID := RandomDatasetID()
	for _, b := range data {
		if err := tx.Process(parentID, b); err != nil {
			t.Fatal(err)
		}
	}

	got, err := BlocksFromCache(c)
	if err != nil {
		t.Fatal(err)
	}

	NormalizeBlocks(got)
	NormalizeBlocks(want)

	sort.Sort(SortedBlocks(got))
	sort.Sort(SortedBlocks(want))

	if !cmp.Equal(want, got, cmpopts.EquateNaNs()) {
		t.Errorf("unexpected blocks -want/+got\n%s", cmp.Diff(want, got))
	}
}
