package functions_test

import (
	"testing"

	"github.com/influxdata/ifql/functions"
	"github.com/influxdata/ifql/query"
	"github.com/influxdata/ifql/query/execute"
	"github.com/influxdata/ifql/query/execute/executetest"
	"github.com/influxdata/ifql/query/plan"
	"github.com/influxdata/ifql/query/plan/plantest"
	"github.com/influxdata/ifql/query/querytest"
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
		data []execute.Block
		want []*executetest.Block
	}{
		{
			name: "fan in",
			spec: &functions.GroupProcedureSpec{
				By: []string{"t1"},
			},
			data: []execute.Block{
				&executetest.Block{
					KeyCols: []string{"t1", "t2"},
					ColMeta: []execute.ColMeta{
						{Label: "_time", Type: execute.TTime},
						{Label: "_value", Type: execute.TFloat},
						{Label: "t1", Type: execute.TString},
						{Label: "t2", Type: execute.TString},
					},
					Data: [][]interface{}{
						{execute.Time(1), 2.0, "a", "x"},
					},
				},
				&executetest.Block{
					KeyCols: []string{"t1", "t2"},
					ColMeta: []execute.ColMeta{
						{Label: "_time", Type: execute.TTime},
						{Label: "_value", Type: execute.TFloat},
						{Label: "t1", Type: execute.TString},
						{Label: "t2", Type: execute.TString},
					},
					Data: [][]interface{}{
						{execute.Time(2), 1.0, "a", "y"},
					},
				},
				&executetest.Block{
					KeyCols: []string{"t1", "t2"},
					ColMeta: []execute.ColMeta{
						{Label: "_time", Type: execute.TTime},
						{Label: "_value", Type: execute.TFloat},
						{Label: "t1", Type: execute.TString},
						{Label: "t2", Type: execute.TString},
					},
					Data: [][]interface{}{
						{execute.Time(1), 4.0, "b", "x"},
					},
				},
				&executetest.Block{
					KeyCols: []string{"t1", "t2"},
					ColMeta: []execute.ColMeta{
						{Label: "_time", Type: execute.TTime},
						{Label: "_value", Type: execute.TFloat},
						{Label: "t1", Type: execute.TString},
						{Label: "t2", Type: execute.TString},
					},
					Data: [][]interface{}{
						{execute.Time(2), 7.0, "b", "y"},
					},
				},
			},
			want: []*executetest.Block{
				{
					KeyCols: []string{"t1"},
					ColMeta: []execute.ColMeta{
						{Label: "_time", Type: execute.TTime},
						{Label: "_value", Type: execute.TFloat},
						{Label: "t1", Type: execute.TString},
						{Label: "t2", Type: execute.TString},
					},
					Data: [][]interface{}{
						{execute.Time(1), 2.0, "a", "x"},
						{execute.Time(2), 1.0, "a", "y"},
					},
				},
				{
					KeyCols: []string{"t1"},
					ColMeta: []execute.ColMeta{
						{Label: "_time", Type: execute.TTime},
						{Label: "_value", Type: execute.TFloat},
						{Label: "t1", Type: execute.TString},
						{Label: "t2", Type: execute.TString},
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
				Except: []string{"_time", "_value", "t2"},
			},
			data: []execute.Block{
				&executetest.Block{
					KeyCols: []string{"t1", "t2", "t3"},
					ColMeta: []execute.ColMeta{
						{Label: "_time", Type: execute.TTime},
						{Label: "_value", Type: execute.TFloat},
						{Label: "t1", Type: execute.TString},
						{Label: "t2", Type: execute.TString},
						{Label: "t3", Type: execute.TString},
					},
					Data: [][]interface{}{
						{execute.Time(1), 2.0, "a", "m", "x"},
					},
				},
				&executetest.Block{
					KeyCols: []string{"t1", "t2", "t3"},
					ColMeta: []execute.ColMeta{
						{Label: "_time", Type: execute.TTime},
						{Label: "_value", Type: execute.TFloat},
						{Label: "t1", Type: execute.TString},
						{Label: "t2", Type: execute.TString},
						{Label: "t3", Type: execute.TString},
					},
					Data: [][]interface{}{
						{execute.Time(2), 1.0, "a", "n", "x"},
					},
				},
				&executetest.Block{
					KeyCols: []string{"t1", "t2", "t3"},
					ColMeta: []execute.ColMeta{
						{Label: "_time", Type: execute.TTime},
						{Label: "_value", Type: execute.TFloat},
						{Label: "t1", Type: execute.TString},
						{Label: "t2", Type: execute.TString},
						{Label: "t3", Type: execute.TString},
					},
					Data: [][]interface{}{
						{execute.Time(1), 4.0, "b", "m", "x"},
					},
				},
				&executetest.Block{
					KeyCols: []string{"t1", "t2", "t3"},
					ColMeta: []execute.ColMeta{
						{Label: "_time", Type: execute.TTime},
						{Label: "_value", Type: execute.TFloat},
						{Label: "t1", Type: execute.TString},
						{Label: "t2", Type: execute.TString},
						{Label: "t3", Type: execute.TString},
					},
					Data: [][]interface{}{
						{execute.Time(2), 7.0, "b", "n", "x"},
					},
				},
			},
			want: []*executetest.Block{
				{
					KeyCols: []string{"t1", "t3"},
					ColMeta: []execute.ColMeta{
						{Label: "_time", Type: execute.TTime},
						{Label: "_value", Type: execute.TFloat},
						{Label: "t1", Type: execute.TString},
						{Label: "t2", Type: execute.TString},
						{Label: "t3", Type: execute.TString},
					},
					Data: [][]interface{}{
						{execute.Time(1), 2.0, "a", "m", "x"},
						{execute.Time(2), 1.0, "a", "n", "x"},
					},
				},
				{
					KeyCols: []string{"t1", "t3"},
					ColMeta: []execute.ColMeta{
						{Label: "_time", Type: execute.TTime},
						{Label: "_value", Type: execute.TFloat},
						{Label: "t1", Type: execute.TString},
						{Label: "t2", Type: execute.TString},
						{Label: "t3", Type: execute.TString},
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
				By: []string{"t1"},
			},
			data: []execute.Block{&executetest.Block{
				ColMeta: []execute.ColMeta{
					{Label: "_time", Type: execute.TTime},
					{Label: "_value", Type: execute.TFloat},
					{Label: "t1", Type: execute.TString},
				},
				Data: [][]interface{}{
					{execute.Time(1), 2.0, "a"},
					{execute.Time(2), 1.0, "b"},
				},
			}},
			want: []*executetest.Block{
				{
					KeyCols: []string{"t1"},
					ColMeta: []execute.ColMeta{
						{Label: "_time", Type: execute.TTime},
						{Label: "_value", Type: execute.TFloat},
						{Label: "t1", Type: execute.TString},
					},
					Data: [][]interface{}{
						{execute.Time(1), 2.0, "a"},
					},
				},
				{
					KeyCols: []string{"t1"},
					ColMeta: []execute.ColMeta{
						{Label: "_time", Type: execute.TTime},
						{Label: "_value", Type: execute.TFloat},
						{Label: "t1", Type: execute.TString},
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
				Except: []string{"_time", "_value", "t2"},
			},
			data: []execute.Block{&executetest.Block{
				ColMeta: []execute.ColMeta{
					{Label: "_time", Type: execute.TTime},
					{Label: "_value", Type: execute.TFloat},
					{Label: "t1", Type: execute.TString},
					{Label: "t2", Type: execute.TString},
					{Label: "t3", Type: execute.TString},
				},
				Data: [][]interface{}{
					{execute.Time(1), 2.0, "a", "m", "x"},
					{execute.Time(2), 1.0, "a", "n", "y"},
				},
			}},
			want: []*executetest.Block{
				{
					KeyCols: []string{"t1", "t3"},
					ColMeta: []execute.ColMeta{
						{Label: "_time", Type: execute.TTime},
						{Label: "_value", Type: execute.TFloat},
						{Label: "t1", Type: execute.TString},
						{Label: "t2", Type: execute.TString},
						{Label: "t3", Type: execute.TString},
					},
					Data: [][]interface{}{
						{execute.Time(1), 2.0, "a", "m", "x"},
					},
				},
				{
					KeyCols: []string{"t1", "t3"},
					ColMeta: []execute.ColMeta{
						{Label: "_time", Type: execute.TTime},
						{Label: "_value", Type: execute.TFloat},
						{Label: "t1", Type: execute.TString},
						{Label: "t2", Type: execute.TString},
						{Label: "t3", Type: execute.TString},
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
				func(d execute.Dataset, c execute.BlockBuilderCache) execute.Transformation {
					return functions.NewGroupTransformation(d, c, tc.spec)
				},
			)
		})
	}
}

func TestGroup_PushDown(t *testing.T) {
	spec := &functions.GroupProcedureSpec{
		By: []string{"t1", "t2"},
	}
	root := &plan.Procedure{
		Spec: new(functions.FromProcedureSpec),
	}
	want := &plan.Procedure{
		Spec: &functions.FromProcedureSpec{
			GroupingSet: true,
			MergeAll:    false,
			GroupKeys:   []string{"t1", "t2"},
		},
	}

	plantest.PhysicalPlan_PushDown_TestHelper(t, spec, root, false, want)
}
func TestGroup_PushDown_Duplicate(t *testing.T) {
	spec := &functions.GroupProcedureSpec{
		By: []string{"t1", "t2"},
	}
	root := &plan.Procedure{
		Spec: &functions.FromProcedureSpec{
			GroupingSet: true,
			MergeAll:    true,
		},
	}
	want := &plan.Procedure{
		// Expect the duplicate has been reset to zero values
		Spec: new(functions.FromProcedureSpec),
	}

	plantest.PhysicalPlan_PushDown_TestHelper(t, spec, root, true, want)
}
