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

func TestGroupOperation_Marshaling(t *testing.T) {
	data := []byte(`{"id":"group","kind":"group","spec":{"by":["t1","t2"]}}`)
	op := &query.Operation{
		ID: "group",
		Spec: &functions.GroupOpSpec{
			By: []string{"t1", "t2"},
		},
	}
	querytest.OperationMarshalingTestHelper(t, data, op)
}

func TestGroup_Process(t *testing.T) {
	testCases := []struct {
		name string
		spec *functions.GroupProcedureSpec
		data []query.Table
		want []*executetest.Table
	}{
		{
			name: "fan in",
			spec: &functions.GroupProcedureSpec{
				GroupMode: functions.GroupModeBy,
				GroupKeys: []string{"t1"},
			},
			data: []query.Table{
				&executetest.Table{
					KeyCols: []string{"t1", "t2"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
						{Label: "t1", Type: query.TString},
						{Label: "t2", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(1), 2.0, "a", "x"},
					},
				},
				&executetest.Table{
					KeyCols: []string{"t1", "t2"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
						{Label: "t1", Type: query.TString},
						{Label: "t2", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(2), 1.0, "a", "y"},
					},
				},
				&executetest.Table{
					KeyCols: []string{"t1", "t2"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
						{Label: "t1", Type: query.TString},
						{Label: "t2", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(1), 4.0, "b", "x"},
					},
				},
				&executetest.Table{
					KeyCols: []string{"t1", "t2"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
						{Label: "t1", Type: query.TString},
						{Label: "t2", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(2), 7.0, "b", "y"},
					},
				},
			},
			want: []*executetest.Table{
				{
					KeyCols: []string{"t1"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
						{Label: "t1", Type: query.TString},
						{Label: "t2", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(1), 2.0, "a", "x"},
						{execute.Time(2), 1.0, "a", "y"},
					},
				},
				{
					KeyCols: []string{"t1"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
						{Label: "t1", Type: query.TString},
						{Label: "t2", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(1), 4.0, "b", "x"},
						{execute.Time(2), 7.0, "b", "y"},
					},
				},
			},
		},
		{
			name: "fan in ignoring",
			spec: &functions.GroupProcedureSpec{
				GroupMode: functions.GroupModeExcept,
				GroupKeys: []string{"_time", "_value", "t2"},
			},
			data: []query.Table{
				&executetest.Table{
					KeyCols: []string{"t1", "t2", "t3"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
						{Label: "t1", Type: query.TString},
						{Label: "t2", Type: query.TString},
						{Label: "t3", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(1), 2.0, "a", "m", "x"},
					},
				},
				&executetest.Table{
					KeyCols: []string{"t1", "t2", "t3"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
						{Label: "t1", Type: query.TString},
						{Label: "t2", Type: query.TString},
						{Label: "t3", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(2), 1.0, "a", "n", "x"},
					},
				},
				&executetest.Table{
					KeyCols: []string{"t1", "t2", "t3"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
						{Label: "t1", Type: query.TString},
						{Label: "t2", Type: query.TString},
						{Label: "t3", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(1), 4.0, "b", "m", "x"},
					},
				},
				&executetest.Table{
					KeyCols: []string{"t1", "t2", "t3"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
						{Label: "t1", Type: query.TString},
						{Label: "t2", Type: query.TString},
						{Label: "t3", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(2), 7.0, "b", "n", "x"},
					},
				},
			},
			want: []*executetest.Table{
				{
					KeyCols: []string{"t1", "t3"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
						{Label: "t1", Type: query.TString},
						{Label: "t2", Type: query.TString},
						{Label: "t3", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(1), 2.0, "a", "m", "x"},
						{execute.Time(2), 1.0, "a", "n", "x"},
					},
				},
				{
					KeyCols: []string{"t1", "t3"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
						{Label: "t1", Type: query.TString},
						{Label: "t2", Type: query.TString},
						{Label: "t3", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(1), 4.0, "b", "m", "x"},
						{execute.Time(2), 7.0, "b", "n", "x"},
					},
				},
			},
		},
		{
			name: "fan out",
			spec: &functions.GroupProcedureSpec{
				GroupMode: functions.GroupModeBy,
				GroupKeys: []string{"t1"},
			},
			data: []query.Table{&executetest.Table{
				ColMeta: []query.ColMeta{
					{Label: "_time", Type: query.TTime},
					{Label: "_value", Type: query.TFloat},
					{Label: "t1", Type: query.TString},
				},
				Data: [][]interface{}{
					{execute.Time(1), 2.0, "a"},
					{execute.Time(2), 1.0, "b"},
				},
			}},
			want: []*executetest.Table{
				{
					KeyCols: []string{"t1"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
						{Label: "t1", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(1), 2.0, "a"},
					},
				},
				{
					KeyCols: []string{"t1"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
						{Label: "t1", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(2), 1.0, "b"},
					},
				},
			},
		},
		{
			name: "fan out ignoring",
			spec: &functions.GroupProcedureSpec{
				GroupMode: functions.GroupModeExcept,
				GroupKeys: []string{"_time", "_value", "t2"},
			},
			data: []query.Table{&executetest.Table{
				ColMeta: []query.ColMeta{
					{Label: "_time", Type: query.TTime},
					{Label: "_value", Type: query.TFloat},
					{Label: "t1", Type: query.TString},
					{Label: "t2", Type: query.TString},
					{Label: "t3", Type: query.TString},
				},
				Data: [][]interface{}{
					{execute.Time(1), 2.0, "a", "m", "x"},
					{execute.Time(2), 1.0, "a", "n", "y"},
				},
			}},
			want: []*executetest.Table{
				{
					KeyCols: []string{"t1", "t3"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
						{Label: "t1", Type: query.TString},
						{Label: "t2", Type: query.TString},
						{Label: "t3", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(1), 2.0, "a", "m", "x"},
					},
				},
				{
					KeyCols: []string{"t1", "t3"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
						{Label: "t1", Type: query.TString},
						{Label: "t2", Type: query.TString},
						{Label: "t3", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(2), 1.0, "a", "n", "y"},
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
					return functions.NewGroupTransformation(d, c, tc.spec)
				},
			)
		})
	}
}

func TestGroup_PushDown(t *testing.T) {
	spec := &functions.GroupProcedureSpec{
		GroupMode: functions.GroupModeBy,
		GroupKeys: []string{"t1", "t2"},
	}
	root := &plan.Procedure{
		Spec: new(functions.FromProcedureSpec),
	}
	want := &plan.Procedure{
		Spec: &functions.FromProcedureSpec{
			GroupingSet: true,
			GroupMode:   functions.GroupModeBy,
			GroupKeys:   []string{"t1", "t2"},
		},
	}

	plantest.PhysicalPlan_PushDown_TestHelper(t, spec, root, false, want)
}
func TestGroup_PushDown_Duplicate(t *testing.T) {
	spec := &functions.GroupProcedureSpec{
		GroupMode: functions.GroupModeBy,
		GroupKeys: []string{"t1", "t2"},
	}
	root := &plan.Procedure{
		Spec: &functions.FromProcedureSpec{
			GroupingSet: true,
			GroupMode:   functions.GroupModeAll,
		},
	}
	want := &plan.Procedure{
		// Expect the duplicate has been reset to zero values
		Spec: new(functions.FromProcedureSpec),
	}

	plantest.PhysicalPlan_PushDown_TestHelper(t, spec, root, true, want)
}
