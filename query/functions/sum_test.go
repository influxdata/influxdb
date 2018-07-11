package functions_test

import (
	"testing"

	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/execute/executetest"
	"github.com/influxdata/platform/query/functions"
	"github.com/influxdata/platform/query/plan"
	"github.com/influxdata/platform/query/plan/plantest"
	"github.com/influxdata/platform/query/querytest"
)

func TestSumOperation_Marshaling(t *testing.T) {
	data := []byte(`{"id":"sum","kind":"sum"}`)
	op := &query.Operation{
		ID:   "sum",
		Spec: &functions.SumOpSpec{},
	}

	querytest.OperationMarshalingTestHelper(t, data, op)
}

func TestSum_Process(t *testing.T) {
	executetest.AggFuncTestHelper(t,
		new(functions.SumAgg),
		[]float64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		float64(45),
	)
}

func BenchmarkSum(b *testing.B) {
	executetest.AggFuncBenchmarkHelper(
		b,
		new(functions.SumAgg),
		NormalData,
		10000816.96729983,
	)
}

func TestSum_PushDown_Match(t *testing.T) {
	spec := new(functions.SumProcedureSpec)
	from := new(functions.FromProcedureSpec)

	// Should not match when an aggregate is set
	from.GroupingSet = true
	plantest.PhysicalPlan_PushDown_Match_TestHelper(t, spec, from, []bool{false})

	// Should match when no aggregate is set
	from.GroupingSet = false
	plantest.PhysicalPlan_PushDown_Match_TestHelper(t, spec, from, []bool{true})
}

func TestSum_PushDown(t *testing.T) {
	spec := new(functions.SumProcedureSpec)
	root := &plan.Procedure{
		Spec: new(functions.FromProcedureSpec),
	}
	want := &plan.Procedure{
		Spec: &functions.FromProcedureSpec{
			AggregateSet:    true,
			AggregateMethod: functions.SumKind,
		},
	}

	plantest.PhysicalPlan_PushDown_TestHelper(t, spec, root, false, want)
}
func TestSum_PushDown_Duplicate(t *testing.T) {
	spec := new(functions.SumProcedureSpec)
	root := &plan.Procedure{
		Spec: &functions.FromProcedureSpec{
			AggregateSet:    true,
			AggregateMethod: functions.SumKind,
		},
	}
	want := &plan.Procedure{
		// Expect the duplicate has been reset to zero values
		Spec: new(functions.FromProcedureSpec),
	}

	plantest.PhysicalPlan_PushDown_TestHelper(t, spec, root, true, want)
}
