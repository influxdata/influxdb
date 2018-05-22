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

func TestAggregate_Process(t *testing.T) {
	sumAgg := new(functions.SumAgg)
	countAgg := new(functions.CountAgg)
	testCases := []struct {
		name   string
		agg    execute.Aggregate
		config execute.AggregateConfig
		data   []*executetest.Block
		want   []*executetest.Block
	}{
		{
			name:   "single",
			config: execute.DefaultAggregateConfig,
			agg:    sumAgg,
			data: []*executetest.Block{{
				KeyCols: []string{"_start", "_stop"},
				ColMeta: []query.ColMeta{
					{Label: "_start", Type: query.TTime},
					{Label: "_stop", Type: query.TTime},
					{Label: "_time", Type: query.TTime},
					{Label: "_value", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(0), execute.Time(100), execute.Time(0), 0.0},
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
			want: []*executetest.Block{{
				KeyCols: []string{"_start", "_stop"},
				ColMeta: []query.ColMeta{
					{Label: "_start", Type: query.TTime},
					{Label: "_stop", Type: query.TTime},
					{Label: "_time", Type: query.TTime},
					{Label: "_value", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(0), execute.Time(100), execute.Time(100), 45.0},
				},
			}},
		},
		{
			name: "single use start time",
			config: execute.AggregateConfig{
				Columns: []string{execute.DefaultValueColLabel},
				TimeSrc: execute.DefaultStartColLabel,
				TimeDst: execute.DefaultTimeColLabel,
			},
			agg: sumAgg,
			data: []*executetest.Block{{
				KeyCols: []string{"_start", "_stop"},
				ColMeta: []query.ColMeta{
					{Label: "_start", Type: query.TTime},
					{Label: "_stop", Type: query.TTime},
					{Label: "_time", Type: query.TTime},
					{Label: "_value", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(0), execute.Time(100), execute.Time(0), 0.0},
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
			want: []*executetest.Block{{
				KeyCols: []string{"_start", "_stop"},
				ColMeta: []query.ColMeta{
					{Label: "_start", Type: query.TTime},
					{Label: "_stop", Type: query.TTime},
					{Label: "_time", Type: query.TTime},
					{Label: "_value", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(0), execute.Time(100), execute.Time(0), 45.0},
				},
			}},
		},
		{
			name:   "multiple blocks",
			config: execute.DefaultAggregateConfig,
			agg:    sumAgg,
			data: []*executetest.Block{
				{
					KeyCols: []string{"_start", "_stop"},
					ColMeta: []query.ColMeta{
						{Label: "_start", Type: query.TTime},
						{Label: "_stop", Type: query.TTime},
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
					},
					Data: [][]interface{}{
						{execute.Time(0), execute.Time(100), execute.Time(0), 0.0},
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
						{execute.Time(100), execute.Time(200), execute.Time(100), 10.0},
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
			want: []*executetest.Block{
				{
					KeyCols: []string{"_start", "_stop"},
					ColMeta: []query.ColMeta{
						{Label: "_start", Type: query.TTime},
						{Label: "_stop", Type: query.TTime},
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
					},
					Data: [][]interface{}{
						{execute.Time(0), execute.Time(100), execute.Time(100), 45.0},
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
						{execute.Time(100), execute.Time(200), execute.Time(200), 145.0},
					},
				},
			},
		},
		{
			name:   "multiple blocks with keyed columns",
			config: execute.DefaultAggregateConfig,
			agg:    sumAgg,
			data: []*executetest.Block{
				{
					KeyCols: []string{"_start", "_stop", "t1"},
					ColMeta: []query.ColMeta{
						{Label: "_start", Type: query.TTime},
						{Label: "_stop", Type: query.TTime},
						{Label: "t1", Type: query.TString},
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
					},
					Data: [][]interface{}{
						{execute.Time(0), execute.Time(100), "a", execute.Time(0), 0.0},
						{execute.Time(0), execute.Time(100), "a", execute.Time(10), 1.0},
						{execute.Time(0), execute.Time(100), "a", execute.Time(20), 2.0},
						{execute.Time(0), execute.Time(100), "a", execute.Time(30), 3.0},
						{execute.Time(0), execute.Time(100), "a", execute.Time(40), 4.0},
						{execute.Time(0), execute.Time(100), "a", execute.Time(50), 5.0},
						{execute.Time(0), execute.Time(100), "a", execute.Time(60), 6.0},
						{execute.Time(0), execute.Time(100), "a", execute.Time(70), 7.0},
						{execute.Time(0), execute.Time(100), "a", execute.Time(80), 8.0},
						{execute.Time(0), execute.Time(100), "a", execute.Time(90), 9.0},
					},
				},
				{
					KeyCols: []string{"_start", "_stop", "t1"},
					ColMeta: []query.ColMeta{
						{Label: "_start", Type: query.TTime},
						{Label: "_stop", Type: query.TTime},
						{Label: "t1", Type: query.TString},
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
					},
					Data: [][]interface{}{
						{execute.Time(0), execute.Time(100), "b", execute.Time(0), 0.3},
						{execute.Time(0), execute.Time(100), "b", execute.Time(10), 1.3},
						{execute.Time(0), execute.Time(100), "b", execute.Time(20), 2.3},
						{execute.Time(0), execute.Time(100), "b", execute.Time(30), 3.3},
						{execute.Time(0), execute.Time(100), "b", execute.Time(40), 4.3},
						{execute.Time(0), execute.Time(100), "b", execute.Time(50), 5.3},
						{execute.Time(0), execute.Time(100), "b", execute.Time(60), 6.3},
						{execute.Time(0), execute.Time(100), "b", execute.Time(70), 7.3},
						{execute.Time(0), execute.Time(100), "b", execute.Time(80), 8.3},
						{execute.Time(0), execute.Time(100), "b", execute.Time(90), 9.3},
					},
				},
				{
					KeyCols: []string{"_start", "_stop", "t1"},
					ColMeta: []query.ColMeta{
						{Label: "_start", Type: query.TTime},
						{Label: "_stop", Type: query.TTime},
						{Label: "t1", Type: query.TString},
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
					},
					Data: [][]interface{}{
						{execute.Time(100), execute.Time(200), "a", execute.Time(100), 10.0},
						{execute.Time(100), execute.Time(200), "a", execute.Time(110), 11.0},
						{execute.Time(100), execute.Time(200), "a", execute.Time(120), 12.0},
						{execute.Time(100), execute.Time(200), "a", execute.Time(130), 13.0},
						{execute.Time(100), execute.Time(200), "a", execute.Time(140), 14.0},
						{execute.Time(100), execute.Time(200), "a", execute.Time(150), 15.0},
						{execute.Time(100), execute.Time(200), "a", execute.Time(160), 16.0},
						{execute.Time(100), execute.Time(200), "a", execute.Time(170), 17.0},
						{execute.Time(100), execute.Time(200), "a", execute.Time(180), 18.0},
						{execute.Time(100), execute.Time(200), "a", execute.Time(190), 19.0},
					},
				},
				{
					KeyCols: []string{"_start", "_stop", "t1"},
					ColMeta: []query.ColMeta{
						{Label: "_start", Type: query.TTime},
						{Label: "_stop", Type: query.TTime},
						{Label: "t1", Type: query.TString},
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
					},
					Data: [][]interface{}{
						{execute.Time(100), execute.Time(200), "b", execute.Time(100), 10.3},
						{execute.Time(100), execute.Time(200), "b", execute.Time(110), 11.3},
						{execute.Time(100), execute.Time(200), "b", execute.Time(120), 12.3},
						{execute.Time(100), execute.Time(200), "b", execute.Time(130), 13.3},
						{execute.Time(100), execute.Time(200), "b", execute.Time(140), 14.3},
						{execute.Time(100), execute.Time(200), "b", execute.Time(150), 15.3},
						{execute.Time(100), execute.Time(200), "b", execute.Time(160), 16.3},
						{execute.Time(100), execute.Time(200), "b", execute.Time(170), 17.3},
						{execute.Time(100), execute.Time(200), "b", execute.Time(180), 18.3},
						{execute.Time(100), execute.Time(200), "b", execute.Time(190), 19.3},
					},
				},
			},
			want: []*executetest.Block{
				{
					KeyCols: []string{"_start", "_stop", "t1"},
					ColMeta: []query.ColMeta{
						{Label: "_start", Type: query.TTime},
						{Label: "_stop", Type: query.TTime},
						{Label: "t1", Type: query.TString},
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
					},
					Data: [][]interface{}{
						{execute.Time(0), execute.Time(100), "a", execute.Time(100), 45.0},
					},
				},
				{
					KeyCols: []string{"_start", "_stop", "t1"},
					ColMeta: []query.ColMeta{
						{Label: "_start", Type: query.TTime},
						{Label: "_stop", Type: query.TTime},
						{Label: "t1", Type: query.TString},
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
					},
					Data: [][]interface{}{
						{execute.Time(100), execute.Time(200), "a", execute.Time(200), 145.0},
					},
				},
				{
					KeyCols: []string{"_start", "_stop", "t1"},
					ColMeta: []query.ColMeta{
						{Label: "_start", Type: query.TTime},
						{Label: "_stop", Type: query.TTime},
						{Label: "t1", Type: query.TString},
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
					},
					Data: [][]interface{}{
						{execute.Time(0), execute.Time(100), "b", execute.Time(100), 48.0},
					},
				},
				{
					KeyCols: []string{"_start", "_stop", "t1"},
					ColMeta: []query.ColMeta{
						{Label: "_start", Type: query.TTime},
						{Label: "_stop", Type: query.TTime},
						{Label: "t1", Type: query.TString},
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
					},
					Data: [][]interface{}{
						{execute.Time(100), execute.Time(200), "b", execute.Time(200), 148.0},
					},
				},
			},
		},
		{
			name: "multiple values",
			config: execute.AggregateConfig{
				Columns: []string{"x", "y"},
				TimeSrc: execute.DefaultStopColLabel,
				TimeDst: execute.DefaultTimeColLabel,
			},
			agg: sumAgg,
			data: []*executetest.Block{{
				KeyCols: []string{"_start", "_stop"},
				ColMeta: []query.ColMeta{
					{Label: "_start", Type: query.TTime},
					{Label: "_stop", Type: query.TTime},
					{Label: "_time", Type: query.TTime},
					{Label: "x", Type: query.TFloat},
					{Label: "y", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(0), execute.Time(100), execute.Time(0), 0.0, 0.0},
					{execute.Time(0), execute.Time(100), execute.Time(10), 1.0, -1.0},
					{execute.Time(0), execute.Time(100), execute.Time(20), 2.0, -2.0},
					{execute.Time(0), execute.Time(100), execute.Time(30), 3.0, -3.0},
					{execute.Time(0), execute.Time(100), execute.Time(40), 4.0, -4.0},
					{execute.Time(0), execute.Time(100), execute.Time(50), 5.0, -5.0},
					{execute.Time(0), execute.Time(100), execute.Time(60), 6.0, -6.0},
					{execute.Time(0), execute.Time(100), execute.Time(70), 7.0, -7.0},
					{execute.Time(0), execute.Time(100), execute.Time(80), 8.0, -8.0},
					{execute.Time(0), execute.Time(100), execute.Time(90), 9.0, -9.0},
				},
			}},
			want: []*executetest.Block{{
				KeyCols: []string{"_start", "_stop"},
				ColMeta: []query.ColMeta{
					{Label: "_start", Type: query.TTime},
					{Label: "_stop", Type: query.TTime},
					{Label: "_time", Type: query.TTime},
					{Label: "x", Type: query.TFloat},
					{Label: "y", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(0), execute.Time(100), execute.Time(100), 45.0, -45.0},
				},
			}},
		},
		{
			name: "multiple values changing types",
			config: execute.AggregateConfig{
				Columns: []string{"x", "y"},
				TimeSrc: execute.DefaultStopColLabel,
				TimeDst: execute.DefaultTimeColLabel,
			},
			agg: countAgg,
			data: []*executetest.Block{{
				KeyCols: []string{"_start", "_stop"},
				ColMeta: []query.ColMeta{
					{Label: "_start", Type: query.TTime},
					{Label: "_stop", Type: query.TTime},
					{Label: "_time", Type: query.TTime},
					{Label: "x", Type: query.TFloat},
					{Label: "y", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(0), execute.Time(100), execute.Time(0), 0.0, 0.0},
					{execute.Time(0), execute.Time(100), execute.Time(10), 1.0, -1.0},
					{execute.Time(0), execute.Time(100), execute.Time(20), 2.0, -2.0},
					{execute.Time(0), execute.Time(100), execute.Time(30), 3.0, -3.0},
					{execute.Time(0), execute.Time(100), execute.Time(40), 4.0, -4.0},
					{execute.Time(0), execute.Time(100), execute.Time(50), 5.0, -5.0},
					{execute.Time(0), execute.Time(100), execute.Time(60), 6.0, -6.0},
					{execute.Time(0), execute.Time(100), execute.Time(70), 7.0, -7.0},
					{execute.Time(0), execute.Time(100), execute.Time(80), 8.0, -8.0},
					{execute.Time(0), execute.Time(100), execute.Time(90), 9.0, -9.0},
				},
			}},
			want: []*executetest.Block{{
				KeyCols: []string{"_start", "_stop"},
				ColMeta: []query.ColMeta{
					{Label: "_start", Type: query.TTime},
					{Label: "_stop", Type: query.TTime},
					{Label: "_time", Type: query.TTime},
					{Label: "x", Type: query.TInt},
					{Label: "y", Type: query.TInt},
				},
				Data: [][]interface{}{
					{execute.Time(0), execute.Time(100), execute.Time(100), int64(10), int64(10)},
				},
			}},
		},
	}
	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			d := executetest.NewDataset(executetest.RandomDatasetID())
			c := execute.NewBlockBuilderCache(executetest.UnlimitedAllocator)
			c.SetTriggerSpec(execute.DefaultTriggerSpec)

			agg := execute.NewAggregateTransformation(d, c, tc.agg, tc.config)

			parentID := executetest.RandomDatasetID()
			for _, b := range tc.data {
				if err := agg.Process(parentID, b); err != nil {
					t.Fatal(err)
				}
			}

			got, err := executetest.BlocksFromCache(c)
			if err != nil {
				t.Fatal(err)
			}

			executetest.NormalizeBlocks(got)
			executetest.NormalizeBlocks(tc.want)

			sort.Sort(executetest.SortedBlocks(got))
			sort.Sort(executetest.SortedBlocks(tc.want))

			if !cmp.Equal(tc.want, got, cmpopts.EquateNaNs()) {
				t.Errorf("unexpected blocks -want/+got\n%s", cmp.Diff(tc.want, got))
			}
		})
	}
}
