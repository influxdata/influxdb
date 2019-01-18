package query_test

import (
	"testing"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/execute/executetest"
	"github.com/influxdata/flux/stdlib/universe"
	"github.com/influxdata/influxdb/query/querytest"
)

func TestReadAndTransform(t *testing.T) {
	start := execute.Time(0)
	stop := execute.Time(20)

	testCases := []struct {
		name  string
		addTs func(d execute.Dataset, c execute.TableBuilderCache, s execute.Source) error
		data  []*querytest.Table
		want  []*executetest.Table
	}{
		{
			name: "limit",
			addTs: func(d execute.Dataset, c execute.TableBuilderCache, s execute.Source) error {
				limit := universe.NewLimitTransformation(d, c, &universe.LimitProcedureSpec{N: 2})
				s.AddTransformation(limit)
				return nil
			},
			data: []*querytest.Table{
				{
					Measurement: "m1",
					Field:       "f1",
					ValueType:   flux.TFloat,
					Rows: []*querytest.Row{
						querytest.R(execute.Time(1), 1.0),
						querytest.R(execute.Time(2), 3.0),
						querytest.R(execute.Time(11), 5.0),
						querytest.R(execute.Time(12), 7.0),
					},
				},
				{
					Measurement: "m1",
					Field:       "f2",
					ValueType:   flux.TFloat,
					Rows: []*querytest.Row{
						querytest.R(execute.Time(1), 2.0),
						querytest.R(execute.Time(2), 4.0),
						querytest.R(execute.Time(11), 6.0),
						querytest.R(execute.Time(12), 8.0),
					},
				},
			},
			want: []*executetest.Table{
				{
					KeyCols: []string{"_start", "_stop", "_field", "_measurement"},
					ColMeta: []flux.ColMeta{
						{Label: "_start", Type: flux.TTime},
						{Label: "_stop", Type: flux.TTime},
						{Label: "_time", Type: flux.TTime},
						{Label: "_value", Type: flux.TFloat},
						{Label: "_field", Type: flux.TString},
						{Label: "_measurement", Type: flux.TString},
					},
					Data: [][]interface{}{
						{execute.Time(0), execute.Time(20), execute.Time(1), 1.0, "f1", "m1"},
						{execute.Time(0), execute.Time(20), execute.Time(2), 3.0, "f1", "m1"},
					},
				},
				{
					KeyCols: []string{"_start", "_stop", "_field", "_measurement"},
					ColMeta: []flux.ColMeta{
						{Label: "_start", Type: flux.TTime},
						{Label: "_stop", Type: flux.TTime},
						{Label: "_time", Type: flux.TTime},
						{Label: "_value", Type: flux.TFloat},
						{Label: "_field", Type: flux.TString},
						{Label: "_measurement", Type: flux.TString},
					},
					Data: [][]interface{}{
						{execute.Time(0), execute.Time(20), execute.Time(1), 2.0, "f2", "m1"},
						{execute.Time(0), execute.Time(20), execute.Time(2), 4.0, "f2", "m1"},
					},
				},
			},
		},
		{
			name: "group",
			addTs: func(d execute.Dataset, c execute.TableBuilderCache, s execute.Source) error {
				group := universe.NewGroupTransformation(d, c, &universe.GroupProcedureSpec{
					GroupMode: flux.GroupModeBy,
					GroupKeys: []string{"_measurement"},
				})
				s.AddTransformation(group)
				return nil
			},
			data: []*querytest.Table{
				{
					Measurement: "m1",
					Field:       "f1",
					ValueType:   flux.TFloat,
					Rows: []*querytest.Row{
						querytest.R(execute.Time(1), 1.0),
						querytest.R(execute.Time(2), 3.0),
						querytest.R(execute.Time(11), 5.0),
						querytest.R(execute.Time(12), 7.0),
					},
				},
				{
					Measurement: "m1",
					Field:       "f2",
					ValueType:   flux.TFloat,
					Rows: []*querytest.Row{
						querytest.R(execute.Time(1), 2.0),
						querytest.R(execute.Time(2), 4.0),
						querytest.R(execute.Time(11), 6.0),
						querytest.R(execute.Time(12), 8.0),
					},
				},
			},
			want: []*executetest.Table{
				{
					KeyCols: []string{"_measurement"},
					ColMeta: []flux.ColMeta{
						{Label: "_start", Type: flux.TTime},
						{Label: "_stop", Type: flux.TTime},
						{Label: "_time", Type: flux.TTime},
						{Label: "_value", Type: flux.TFloat},
						{Label: "_field", Type: flux.TString},
						{Label: "_measurement", Type: flux.TString},
					},
					Data: [][]interface{}{
						{execute.Time(0), execute.Time(20), execute.Time(1), 1.0, "f1", "m1"},
						{execute.Time(0), execute.Time(20), execute.Time(2), 3.0, "f1", "m1"},
						{execute.Time(0), execute.Time(20), execute.Time(11), 5.0, "f1", "m1"},
						{execute.Time(0), execute.Time(20), execute.Time(12), 7.0, "f1", "m1"},
						{execute.Time(0), execute.Time(20), execute.Time(1), 2.0, "f2", "m1"},
						{execute.Time(0), execute.Time(20), execute.Time(2), 4.0, "f2", "m1"},
						{execute.Time(0), execute.Time(20), execute.Time(11), 6.0, "f2", "m1"},
						{execute.Time(0), execute.Time(20), execute.Time(12), 8.0, "f2", "m1"},
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			querytest.ReaderTestHelper(
				t,
				start,
				stop,
				tc.addTs,
				tc.data,
				tc.want,
			)
		})
	}
}
