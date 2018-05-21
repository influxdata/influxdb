package functions_test

import (
	"testing"
	"time"

	"github.com/influxdata/ifql/functions"
	"github.com/influxdata/ifql/query"
	"github.com/influxdata/ifql/query/execute"
	"github.com/influxdata/ifql/query/execute/executetest"
	"github.com/influxdata/ifql/query/plan"
	"github.com/influxdata/ifql/query/plan/plantest"
	"github.com/influxdata/ifql/query/querytest"
)

func TestCount_NewQuery(t *testing.T) {
	tests := []querytest.NewQueryTestCase{
		{
			Name: "from with range and count",
			Raw:  `from(db:"mydb") |> range(start:-4h, stop:-2h) |> count()`,
			Want: &query.Spec{
				Operations: []*query.Operation{
					{
						ID: "from0",
						Spec: &functions.FromOpSpec{
							Database: "mydb",
						},
					},
					{
						ID: "range1",
						Spec: &functions.RangeOpSpec{
							Start: query.Time{
								Relative:   -4 * time.Hour,
								IsRelative: true,
							},
							Stop: query.Time{
								Relative:   -2 * time.Hour,
								IsRelative: true,
							},
						},
					},
					{
						ID: "count2",
						Spec: &functions.CountOpSpec{
							AggregateConfig: execute.DefaultAggregateConfig,
						},
					},
				},
				Edges: []query.Edge{
					{Parent: "from0", Child: "range1"},
					{Parent: "range1", Child: "count2"},
				},
			},
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()
			querytest.NewQueryTestHelper(t, tc)
		})
	}
}

func TestCountOperation_Marshaling(t *testing.T) {
	data := []byte(`{"id":"count","kind":"count"}`)
	op := &query.Operation{
		ID:   "count",
		Spec: &functions.CountOpSpec{},
	}

	querytest.OperationMarshalingTestHelper(t, data, op)
}

func TestCount_Process(t *testing.T) {
	executetest.AggFuncTestHelper(
		t,
		new(functions.CountAgg),
		[]float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		int64(10),
	)
}
func BenchmarkCount(b *testing.B) {
	executetest.AggFuncBenchmarkHelper(
		b,
		new(functions.CountAgg),
		NormalData,
		int64(len(NormalData)),
	)
}

func TestCount_PushDown_Match(t *testing.T) {
	spec := new(functions.CountProcedureSpec)
	from := new(functions.FromProcedureSpec)

	// Should not match when an aggregate is set
	from.GroupingSet = true
	plantest.PhysicalPlan_PushDown_Match_TestHelper(t, spec, from, []bool{false})

	// Should match when no aggregate is set
	from.GroupingSet = false
	plantest.PhysicalPlan_PushDown_Match_TestHelper(t, spec, from, []bool{true})
}

func TestCount_PushDown(t *testing.T) {
	spec := new(functions.CountProcedureSpec)
	root := &plan.Procedure{
		Spec: new(functions.FromProcedureSpec),
	}
	want := &plan.Procedure{
		Spec: &functions.FromProcedureSpec{
			AggregateSet:    true,
			AggregateMethod: functions.CountKind,
		},
	}

	plantest.PhysicalPlan_PushDown_TestHelper(t, spec, root, false, want)
}

func TestCount_PushDown_Duplicate(t *testing.T) {
	spec := new(functions.CountProcedureSpec)
	root := &plan.Procedure{
		Spec: &functions.FromProcedureSpec{
			AggregateSet:    true,
			AggregateMethod: functions.CountKind,
		},
	}
	want := &plan.Procedure{
		// Expect the duplicate has been reset to zero values
		Spec: new(functions.FromProcedureSpec),
	}

	plantest.PhysicalPlan_PushDown_TestHelper(t, spec, root, true, want)
}
