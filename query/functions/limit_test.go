package functions_test

import (
	"testing"

	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/execute"
	"github.com/influxdata/platform/query/execute/executetest"
	"github.com/influxdata/platform/query/functions"
	"github.com/influxdata/platform/query/plan"
	"github.com/influxdata/platform/query/plan/plantest"
	"github.com/influxdata/platform/query/querytest"
)

func TestLimitOperation_Marshaling(t *testing.T) {
	data := []byte(`{"id":"limit","kind":"limit","spec":{"n":10}}`)
	op := &query.Operation{
		ID: "limit",
		Spec: &functions.LimitOpSpec{
			N: 10,
		},
	}

	querytest.OperationMarshalingTestHelper(t, data, op)
}

func TestLimit_Process(t *testing.T) {
	testCases := []struct {
		name string
		spec *functions.LimitProcedureSpec
		data []query.Table
		want []*executetest.Table
	}{
		{
			name: "one table",
			spec: &functions.LimitProcedureSpec{
				N: 1,
			},
			data: []query.Table{&executetest.Table{
				ColMeta: []query.ColMeta{
					{Label: "_time", Type: query.TTime},
					{Label: "_value", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(1), 2.0},
					{execute.Time(2), 1.0},
				},
			}},
			want: []*executetest.Table{{
				ColMeta: []query.ColMeta{
					{Label: "_time", Type: query.TTime},
					{Label: "_value", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(1), 2.0},
				},
			}},
		},
		{
			name: "one table with offset single batch",
			spec: &functions.LimitProcedureSpec{
				N:      1,
				Offset: 1,
			},
			data: []query.Table{execute.CopyTable(&executetest.Table{
				ColMeta: []query.ColMeta{
					{Label: "_time", Type: query.TTime},
					{Label: "_value", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(1), 2.0},
					{execute.Time(2), 1.0},
					{execute.Time(3), 0.0},
				},
			}, executetest.UnlimitedAllocator)},
			want: []*executetest.Table{{
				ColMeta: []query.ColMeta{
					{Label: "_time", Type: query.TTime},
					{Label: "_value", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(2), 1.0},
				},
			}},
		},
		{
			name: "one table with offset multiple batches",
			spec: &functions.LimitProcedureSpec{
				N:      1,
				Offset: 1,
			},
			data: []query.Table{&executetest.Table{
				ColMeta: []query.ColMeta{
					{Label: "_time", Type: query.TTime},
					{Label: "_value", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(1), 2.0},
					{execute.Time(2), 1.0},
					{execute.Time(3), 0.0},
				},
			}},
			want: []*executetest.Table{{
				ColMeta: []query.ColMeta{
					{Label: "_time", Type: query.TTime},
					{Label: "_value", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(2), 1.0},
				},
			}},
		},
		{
			name: "multiple tables",
			spec: &functions.LimitProcedureSpec{
				N: 2,
			},
			data: []query.Table{
				&executetest.Table{
					KeyCols: []string{"t1"},
					ColMeta: []query.ColMeta{
						{Label: "t1", Type: query.TString},
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
					},
					Data: [][]interface{}{
						{"a", execute.Time(1), 3.0},
						{"a", execute.Time(2), 2.0},
						{"a", execute.Time(2), 1.0},
					},
				},
				&executetest.Table{
					KeyCols: []string{"t1"},
					ColMeta: []query.ColMeta{
						{Label: "t1", Type: query.TString},
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
					},
					Data: [][]interface{}{
						{"b", execute.Time(3), 3.0},
						{"b", execute.Time(3), 2.0},
						{"b", execute.Time(4), 1.0},
					},
				},
			},
			want: []*executetest.Table{
				{
					KeyCols: []string{"t1"},
					ColMeta: []query.ColMeta{
						{Label: "t1", Type: query.TString},
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
					},
					Data: [][]interface{}{
						{"a", execute.Time(1), 3.0},
						{"a", execute.Time(2), 2.0},
					},
				},
				{
					KeyCols: []string{"t1"},
					ColMeta: []query.ColMeta{
						{Label: "t1", Type: query.TString},
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
					},
					Data: [][]interface{}{
						{"b", execute.Time(3), 3.0},
						{"b", execute.Time(3), 2.0},
					},
				},
			},
		},
	}
	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			executetest.ProcessTestHelper(
				t,
				tc.data,
				tc.want,
				func(d execute.Dataset, c execute.TableBuilderCache) execute.Transformation {
					return functions.NewLimitTransformation(d, c, tc.spec)
				},
			)
		})
	}
}

func TestLimit_PushDown(t *testing.T) {
	spec := &functions.LimitProcedureSpec{
		N: 42,
	}
	root := &plan.Procedure{
		Spec: new(functions.FromProcedureSpec),
	}
	want := &plan.Procedure{
		Spec: &functions.FromProcedureSpec{
			LimitSet:    true,
			PointsLimit: 42,
		},
	}

	plantest.PhysicalPlan_PushDown_TestHelper(t, spec, root, false, want)
}
func TestLimit_PushDown_Duplicate(t *testing.T) {
	spec := &functions.LimitProcedureSpec{
		N: 9,
	}
	root := &plan.Procedure{
		Spec: &functions.FromProcedureSpec{
			LimitSet:    true,
			PointsLimit: 42,
		},
	}
	want := &plan.Procedure{
		// Expect the duplicate has been reset to zero values
		Spec: new(functions.FromProcedureSpec),
	}

	plantest.PhysicalPlan_PushDown_TestHelper(t, spec, root, true, want)
}
