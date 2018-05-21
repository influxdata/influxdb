package functions_test

import (
	"testing"
	"time"

	"github.com/influxdata/platform/query/functions"
	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/execute"
	"github.com/influxdata/platform/query/execute/executetest"
	"github.com/influxdata/platform/query/querytest"
)

func TestIntegralOperation_Marshaling(t *testing.T) {
	data := []byte(`{"id":"integral","kind":"integral","spec":{"unit":"1m"}}`)
	op := &query.Operation{
		ID: "integral",
		Spec: &functions.IntegralOpSpec{
			Unit: query.Duration(time.Minute),
		},
	}
	querytest.OperationMarshalingTestHelper(t, data, op)
}

func TestIntegral_PassThrough(t *testing.T) {
	executetest.TransformationPassThroughTestHelper(t, func(d execute.Dataset, c execute.BlockBuilderCache) execute.Transformation {
		s := functions.NewIntegralTransformation(
			d,
			c,
			&functions.IntegralProcedureSpec{},
		)
		return s
	})
}

func TestIntegral_Process(t *testing.T) {
	testCases := []struct {
		name string
		spec *functions.IntegralProcedureSpec
		data []execute.Block
		want []*executetest.Block
	}{
		{
			name: "float",
			spec: &functions.IntegralProcedureSpec{
				Unit:            1,
				AggregateConfig: execute.DefaultAggregateConfig,
			},
			data: []execute.Block{&executetest.Block{
				KeyCols: []string{"_start", "_stop"},
				ColMeta: []execute.ColMeta{
					{Label: "_start", Type: execute.TTime},
					{Label: "_stop", Type: execute.TTime},
					{Label: "_time", Type: execute.TTime},
					{Label: "_value", Type: execute.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(1), execute.Time(3), execute.Time(1), 2.0},
					{execute.Time(1), execute.Time(3), execute.Time(2), 1.0},
				},
			}},
			want: []*executetest.Block{{
				KeyCols: []string{"_start", "_stop"},
				ColMeta: []execute.ColMeta{
					{Label: "_start", Type: execute.TTime},
					{Label: "_stop", Type: execute.TTime},
					{Label: "_time", Type: execute.TTime},
					{Label: "_value", Type: execute.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(1), execute.Time(3), execute.Time(3), 1.5},
				},
			}},
		},
		{
			name: "float with units",
			spec: &functions.IntegralProcedureSpec{
				Unit:            query.Duration(time.Second),
				AggregateConfig: execute.DefaultAggregateConfig,
			},
			data: []execute.Block{&executetest.Block{
				KeyCols: []string{"_start", "_stop"},
				ColMeta: []execute.ColMeta{
					{Label: "_start", Type: execute.TTime},
					{Label: "_stop", Type: execute.TTime},
					{Label: "_time", Type: execute.TTime},
					{Label: "_value", Type: execute.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(1 * time.Second), execute.Time(4 * time.Second), execute.Time(1 * time.Second), 2.0},
					{execute.Time(1 * time.Second), execute.Time(4 * time.Second), execute.Time(3 * time.Second), 1.0},
				},
			}},
			want: []*executetest.Block{{
				KeyCols: []string{"_start", "_stop"},
				ColMeta: []execute.ColMeta{
					{Label: "_start", Type: execute.TTime},
					{Label: "_stop", Type: execute.TTime},
					{Label: "_time", Type: execute.TTime},
					{Label: "_value", Type: execute.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(1 * time.Second), execute.Time(4 * time.Second), execute.Time(4 * time.Second), 3.0},
				},
			}},
		},
		{
			name: "float with tags",
			spec: &functions.IntegralProcedureSpec{
				Unit:            1,
				AggregateConfig: execute.DefaultAggregateConfig,
			},
			data: []execute.Block{&executetest.Block{
				KeyCols: []string{"_start", "_stop"},
				ColMeta: []execute.ColMeta{
					{Label: "_start", Type: execute.TTime},
					{Label: "_stop", Type: execute.TTime},
					{Label: "_time", Type: execute.TTime},
					{Label: "_value", Type: execute.TFloat},
					{Label: "t", Type: execute.TString},
				},
				Data: [][]interface{}{
					{execute.Time(1), execute.Time(3), execute.Time(1), 2.0, "a"},
					{execute.Time(1), execute.Time(3), execute.Time(2), 1.0, "b"},
				},
			}},
			want: []*executetest.Block{{
				KeyCols: []string{"_start", "_stop"},
				ColMeta: []execute.ColMeta{
					{Label: "_start", Type: execute.TTime},
					{Label: "_stop", Type: execute.TTime},
					{Label: "_time", Type: execute.TTime},
					{Label: "_value", Type: execute.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(1), execute.Time(3), execute.Time(3), 1.5},
				},
			}},
		},
		{
			name: "float with multiple values",
			spec: &functions.IntegralProcedureSpec{
				Unit: 1,
				AggregateConfig: execute.AggregateConfig{
					TimeDst: execute.DefaultTimeColLabel,
					TimeSrc: execute.DefaultStopColLabel,
					Columns: []string{"x", "y"},
				},
			},
			data: []execute.Block{&executetest.Block{
				KeyCols: []string{"_start", "_stop"},
				ColMeta: []execute.ColMeta{
					{Label: "_start", Type: execute.TTime},
					{Label: "_stop", Type: execute.TTime},
					{Label: "_time", Type: execute.TTime},
					{Label: "x", Type: execute.TFloat},
					{Label: "y", Type: execute.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(1), execute.Time(5), execute.Time(1), 2.0, 20.0},
					{execute.Time(1), execute.Time(5), execute.Time(2), 1.0, 10.0},
					{execute.Time(1), execute.Time(5), execute.Time(3), 2.0, 20.0},
					{execute.Time(1), execute.Time(5), execute.Time(4), 1.0, 10.0},
				},
			}},
			want: []*executetest.Block{{
				KeyCols: []string{"_start", "_stop"},
				ColMeta: []execute.ColMeta{
					{Label: "_start", Type: execute.TTime},
					{Label: "_stop", Type: execute.TTime},
					{Label: "_time", Type: execute.TTime},
					{Label: "x", Type: execute.TFloat},
					{Label: "y", Type: execute.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(1), execute.Time(5), execute.Time(5), 4.5, 45.0},
				},
			}},
		},
	}
	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			executetest.ProcessTestHelper(
				t,
				tc.data,
				tc.want,
				func(d execute.Dataset, c execute.BlockBuilderCache) execute.Transformation {
					return functions.NewIntegralTransformation(d, c, tc.spec)
				},
			)
		})
	}
}
