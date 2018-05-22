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

func TestLastOperation_Marshaling(t *testing.T) {
	data := []byte(`{"id":"last","kind":"last","spec":{"column":"bar"}}`)
	op := &query.Operation{
		ID: "last",
		Spec: &functions.LastOpSpec{
			SelectorConfig: execute.SelectorConfig{
				Column: "bar",
			},
		},
	}

	querytest.OperationMarshalingTestHelper(t, data, op)
}

func TestLast_Process(t *testing.T) {
	testCases := []struct {
		name string
		data *executetest.Block
		want []execute.Row
	}{
		{
			name: "last",
			data: &executetest.Block{
				KeyCols: []string{"t1"},
				ColMeta: []query.ColMeta{
					{Label: "_time", Type: query.TTime},
					{Label: "_value", Type: query.TFloat},
					{Label: "t1", Type: query.TString},
					{Label: "t2", Type: query.TString},
				},
				Data: [][]interface{}{
					{execute.Time(0), 0.0, "a", "y"},
					{execute.Time(10), 5.0, "a", "x"},
					{execute.Time(20), 9.0, "a", "y"},
					{execute.Time(30), 4.0, "a", "x"},
					{execute.Time(40), 6.0, "a", "y"},
					{execute.Time(50), 8.0, "a", "x"},
					{execute.Time(60), 1.0, "a", "y"},
					{execute.Time(70), 2.0, "a", "x"},
					{execute.Time(80), 3.0, "a", "y"},
					{execute.Time(90), 7.0, "a", "x"},
				},
			},
			want: []execute.Row{{
				Values: []interface{}{execute.Time(90), 7.0, "a", "x"},
			}},
		},
	}
	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			executetest.RowSelectorFuncTestHelper(
				t,
				new(functions.LastSelector),
				tc.data,
				tc.want,
			)
		})
	}
}

func BenchmarkLast(b *testing.B) {
	executetest.RowSelectorFuncBenchmarkHelper(b, new(functions.LastSelector), NormalBlock)
}

func TestLast_PushDown_Match(t *testing.T) {
	spec := new(functions.LastProcedureSpec)
	from := new(functions.FromProcedureSpec)

	// Should not match when an aggregate is set
	from.AggregateSet = true
	plantest.PhysicalPlan_PushDown_Match_TestHelper(t, spec, from, []bool{false})

	// Should match when no aggregate is set
	from.AggregateSet = false
	plantest.PhysicalPlan_PushDown_Match_TestHelper(t, spec, from, []bool{true})
}

func TestLast_PushDown(t *testing.T) {
	spec := new(functions.LastProcedureSpec)
	root := &plan.Procedure{
		Spec: new(functions.FromProcedureSpec),
	}
	want := &plan.Procedure{
		Spec: &functions.FromProcedureSpec{
			BoundsSet: true,
			Bounds: plan.BoundsSpec{
				Start: query.MinTime,
				Stop:  query.Now,
			},
			LimitSet:      true,
			PointsLimit:   1,
			DescendingSet: true,
			Descending:    true,
		},
	}

	plantest.PhysicalPlan_PushDown_TestHelper(t, spec, root, false, want)
}
func TestLast_PushDown_Duplicate(t *testing.T) {
	spec := new(functions.LastProcedureSpec)
	root := &plan.Procedure{
		Spec: &functions.FromProcedureSpec{
			BoundsSet: true,
			Bounds: plan.BoundsSpec{
				Start: query.MinTime,
				Stop:  query.Now,
			},
			LimitSet:      true,
			PointsLimit:   1,
			DescendingSet: true,
			Descending:    true,
		},
	}
	want := &plan.Procedure{
		// Expect the duplicate has been reset to zero values
		Spec: new(functions.FromProcedureSpec),
	}

	plantest.PhysicalPlan_PushDown_TestHelper(t, spec, root, true, want)
}
