package execute_test

import (
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/execute"
	"github.com/influxdata/platform/query/execute/executetest"
	"github.com/influxdata/platform/query/functions"
)

func TestRowSelector_Process(t *testing.T) {
	// All test cases use a simple MinSelector
	testCases := []struct {
		name   string
		config execute.SelectorConfig
		data   []*executetest.Table
		want   []*executetest.Table
	}{
		{
			name: "single",
			config: execute.SelectorConfig{
				Column: "_value",
			},
			data: []*executetest.Table{{
				KeyCols: []string{"_start", "_stop"},
				ColMeta: []query.ColMeta{
					{Label: "_start", Type: query.TTime},
					{Label: "_stop", Type: query.TTime},
					{Label: "_time", Type: query.TTime},
					{Label: "_value", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(0), execute.Time(100), execute.Time(1), 0.0},
					{execute.Time(0), execute.Time(100), execute.Time(10), 1.0},
					{execute.Time(0), execute.Time(100), execute.Time(20), 2.0},
					{execute.Time(0), execute.Time(100), execute.Time(30), 3.0},
					{execute.Time(0), execute.Time(100), execute.Time(40), 4.0},
					{execute.Time(0), execute.Time(100), execute.Time(50), 5.0},
					{execute.Time(0), execute.Time(100), execute.Time(60), 6.0},
					{execute.Time(0), execute.Time(100), execute.Time(70), 7.0},
					{execute.Time(0), execute.Time(100), execute.Time(80), 8.0},
					{execute.Time(0), execute.Time(100), execute.Time(90), 9.0},
				},
			}},
			want: []*executetest.Table{{
				KeyCols: []string{"_start", "_stop"},
				ColMeta: []query.ColMeta{
					{Label: "_start", Type: query.TTime},
					{Label: "_stop", Type: query.TTime},
					{Label: "_time", Type: query.TTime},
					{Label: "_value", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(0), execute.Time(100), execute.Time(1), 0.0},
				},
			}},
		},
		{
			name: "single custom column",
			config: execute.SelectorConfig{
				Column: "x",
			},
			data: []*executetest.Table{{
				KeyCols: []string{"_start", "_stop"},
				ColMeta: []query.ColMeta{
					{Label: "_start", Type: query.TTime},
					{Label: "_stop", Type: query.TTime},
					{Label: "_time", Type: query.TTime},
					{Label: "x", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(0), execute.Time(100), execute.Time(1), 0.0},
					{execute.Time(0), execute.Time(100), execute.Time(10), 1.0},
					{execute.Time(0), execute.Time(100), execute.Time(20), 2.0},
					{execute.Time(0), execute.Time(100), execute.Time(30), 3.0},
					{execute.Time(0), execute.Time(100), execute.Time(40), 4.0},
					{execute.Time(0), execute.Time(100), execute.Time(50), 5.0},
					{execute.Time(0), execute.Time(100), execute.Time(60), 6.0},
					{execute.Time(0), execute.Time(100), execute.Time(70), 7.0},
					{execute.Time(0), execute.Time(100), execute.Time(80), 8.0},
					{execute.Time(0), execute.Time(100), execute.Time(90), 9.0},
				},
			}},
			want: []*executetest.Table{{
				KeyCols: []string{"_start", "_stop"},
				ColMeta: []query.ColMeta{
					{Label: "_start", Type: query.TTime},
					{Label: "_stop", Type: query.TTime},
					{Label: "_time", Type: query.TTime},
					{Label: "x", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(0), execute.Time(100), execute.Time(1), 0.0},
				},
			}},
		},
		{
			name: "multiple tables",
			config: execute.SelectorConfig{
				Column: "_value",
			},
			data: []*executetest.Table{
				{
					KeyCols: []string{"_start", "_stop"},
					ColMeta: []query.ColMeta{
						{Label: "_start", Type: query.TTime},
						{Label: "_stop", Type: query.TTime},
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
					},
					Data: [][]interface{}{
						{execute.Time(0), execute.Time(100), execute.Time(1), 0.0},
						{execute.Time(0), execute.Time(100), execute.Time(10), 1.0},
						{execute.Time(0), execute.Time(100), execute.Time(20), 2.0},
						{execute.Time(0), execute.Time(100), execute.Time(30), 3.0},
						{execute.Time(0), execute.Time(100), execute.Time(40), 4.0},
						{execute.Time(0), execute.Time(100), execute.Time(50), 5.0},
						{execute.Time(0), execute.Time(100), execute.Time(60), 6.0},
						{execute.Time(0), execute.Time(100), execute.Time(70), 7.0},
						{execute.Time(0), execute.Time(100), execute.Time(80), 8.0},
						{execute.Time(0), execute.Time(100), execute.Time(90), 9.0},
					},
				},
				{
					KeyCols: []string{"_start", "_stop"},
					ColMeta: []query.ColMeta{
						{Label: "_start", Type: query.TTime},
						{Label: "_stop", Type: query.TTime},
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
					},
					Data: [][]interface{}{
						{execute.Time(100), execute.Time(200), execute.Time(101), 10.0},
						{execute.Time(100), execute.Time(200), execute.Time(110), 11.0},
						{execute.Time(100), execute.Time(200), execute.Time(120), 12.0},
						{execute.Time(100), execute.Time(200), execute.Time(130), 13.0},
						{execute.Time(100), execute.Time(200), execute.Time(140), 14.0},
						{execute.Time(100), execute.Time(200), execute.Time(150), 15.0},
						{execute.Time(100), execute.Time(200), execute.Time(160), 16.0},
						{execute.Time(100), execute.Time(200), execute.Time(170), 17.0},
						{execute.Time(100), execute.Time(200), execute.Time(180), 18.0},
						{execute.Time(100), execute.Time(200), execute.Time(190), 19.0},
					},
				},
			},
			want: []*executetest.Table{
				{
					KeyCols: []string{"_start", "_stop"},
					ColMeta: []query.ColMeta{
						{Label: "_start", Type: query.TTime},
						{Label: "_stop", Type: query.TTime},
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
					},
					Data: [][]interface{}{
						{execute.Time(0), execute.Time(100), execute.Time(1), 0.0},
					},
				},
				{
					KeyCols: []string{"_start", "_stop"},
					ColMeta: []query.ColMeta{
						{Label: "_start", Type: query.TTime},
						{Label: "_stop", Type: query.TTime},
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
					},
					Data: [][]interface{}{
						{execute.Time(100), execute.Time(200), execute.Time(101), 10.0},
					},
				},
			},
		},
	}
	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			d := executetest.NewDataset(executetest.RandomDatasetID())
			c := execute.NewTableBuilderCache(executetest.UnlimitedAllocator)
			c.SetTriggerSpec(execute.DefaultTriggerSpec)

			selector := execute.NewRowSelectorTransformation(d, c, new(functions.MinSelector), tc.config)

			parentID := executetest.RandomDatasetID()
			for _, b := range tc.data {
				if err := selector.Process(parentID, b); err != nil {
					t.Fatal(err)
				}
			}

			got, err := executetest.TablesFromCache(c)
			if err != nil {
				t.Fatal(err)
			}

			executetest.NormalizeTables(got)
			executetest.NormalizeTables(tc.want)

			sort.Sort(executetest.SortedTables(got))
			sort.Sort(executetest.SortedTables(tc.want))

			if !cmp.Equal(tc.want, got, cmpopts.EquateNaNs()) {
				t.Errorf("unexpected tables -want/+got\n%s", cmp.Diff(tc.want, got))
			}
		})
	}
}

func TestIndexSelector_Process(t *testing.T) {
	// All test cases use a simple FirstSelector
	testCases := []struct {
		name   string
		config execute.SelectorConfig
		data   []*executetest.Table
		want   []*executetest.Table
	}{
		{
			name: "single",
			config: execute.SelectorConfig{
				Column: "_value",
			},
			data: []*executetest.Table{{
				KeyCols: []string{"_start", "_stop"},
				ColMeta: []query.ColMeta{
					{Label: "_start", Type: query.TTime},
					{Label: "_stop", Type: query.TTime},
					{Label: "_time", Type: query.TTime},
					{Label: "_value", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(0), execute.Time(100), execute.Time(1), 0.0},
					{execute.Time(0), execute.Time(100), execute.Time(10), 1.0},
					{execute.Time(0), execute.Time(100), execute.Time(20), 2.0},
					{execute.Time(0), execute.Time(100), execute.Time(30), 3.0},
					{execute.Time(0), execute.Time(100), execute.Time(40), 4.0},
					{execute.Time(0), execute.Time(100), execute.Time(50), 5.0},
					{execute.Time(0), execute.Time(100), execute.Time(60), 6.0},
					{execute.Time(0), execute.Time(100), execute.Time(70), 7.0},
					{execute.Time(0), execute.Time(100), execute.Time(80), 8.0},
					{execute.Time(0), execute.Time(100), execute.Time(90), 9.0},
				},
			}},
			want: []*executetest.Table{{
				KeyCols: []string{"_start", "_stop"},
				ColMeta: []query.ColMeta{
					{Label: "_start", Type: query.TTime},
					{Label: "_stop", Type: query.TTime},
					{Label: "_time", Type: query.TTime},
					{Label: "_value", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(0), execute.Time(100), execute.Time(1), 0.0},
				},
			}},
		},
		{
			name: "multiple tables",
			config: execute.SelectorConfig{
				Column: "_value",
			},
			data: []*executetest.Table{
				{
					KeyCols: []string{"_start", "_stop"},
					ColMeta: []query.ColMeta{
						{Label: "_start", Type: query.TTime},
						{Label: "_stop", Type: query.TTime},
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
					},
					Data: [][]interface{}{
						{execute.Time(0), execute.Time(100), execute.Time(1), 0.0},
						{execute.Time(0), execute.Time(100), execute.Time(10), 1.0},
						{execute.Time(0), execute.Time(100), execute.Time(20), 2.0},
						{execute.Time(0), execute.Time(100), execute.Time(30), 3.0},
						{execute.Time(0), execute.Time(100), execute.Time(40), 4.0},
						{execute.Time(0), execute.Time(100), execute.Time(50), 5.0},
						{execute.Time(0), execute.Time(100), execute.Time(60), 6.0},
						{execute.Time(0), execute.Time(100), execute.Time(70), 7.0},
						{execute.Time(0), execute.Time(100), execute.Time(80), 8.0},
						{execute.Time(0), execute.Time(100), execute.Time(90), 9.0},
					},
				},
				{
					KeyCols: []string{"_start", "_stop"},
					ColMeta: []query.ColMeta{
						{Label: "_start", Type: query.TTime},
						{Label: "_stop", Type: query.TTime},
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
					},
					Data: [][]interface{}{
						{execute.Time(100), execute.Time(200), execute.Time(101), 10.0},
						{execute.Time(100), execute.Time(200), execute.Time(110), 11.0},
						{execute.Time(100), execute.Time(200), execute.Time(120), 12.0},
						{execute.Time(100), execute.Time(200), execute.Time(130), 13.0},
						{execute.Time(100), execute.Time(200), execute.Time(140), 14.0},
						{execute.Time(100), execute.Time(200), execute.Time(150), 15.0},
						{execute.Time(100), execute.Time(200), execute.Time(160), 16.0},
						{execute.Time(100), execute.Time(200), execute.Time(170), 17.0},
						{execute.Time(100), execute.Time(200), execute.Time(180), 18.0},
						{execute.Time(100), execute.Time(200), execute.Time(190), 19.0},
					},
				},
			},
			want: []*executetest.Table{
				{
					KeyCols: []string{"_start", "_stop"},
					ColMeta: []query.ColMeta{
						{Label: "_start", Type: query.TTime},
						{Label: "_stop", Type: query.TTime},
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
					},
					Data: [][]interface{}{
						{execute.Time(0), execute.Time(100), execute.Time(1), 0.0},
					},
				},
				{
					KeyCols: []string{"_start", "_stop"},
					ColMeta: []query.ColMeta{
						{Label: "_start", Type: query.TTime},
						{Label: "_stop", Type: query.TTime},
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
					},
					Data: [][]interface{}{
						{execute.Time(100), execute.Time(200), execute.Time(101), 10.0},
					},
				},
			},
		},
	}
	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			d := executetest.NewDataset(executetest.RandomDatasetID())
			c := execute.NewTableBuilderCache(executetest.UnlimitedAllocator)
			c.SetTriggerSpec(execute.DefaultTriggerSpec)

			selector := execute.NewIndexSelectorTransformation(d, c, new(functions.FirstSelector), tc.config)

			parentID := executetest.RandomDatasetID()
			for _, b := range tc.data {
				if err := selector.Process(parentID, b); err != nil {
					t.Fatal(err)
				}
			}

			got, err := executetest.TablesFromCache(c)
			if err != nil {
				t.Fatal(err)
			}

			executetest.NormalizeTables(got)
			executetest.NormalizeTables(tc.want)

			sort.Sort(executetest.SortedTables(got))
			sort.Sort(executetest.SortedTables(tc.want))

			if !cmp.Equal(tc.want, got, cmpopts.EquateNaNs()) {
				t.Errorf("unexpected tables -want/+got\n%s", cmp.Diff(tc.want, got))
			}
		})
	}
}
