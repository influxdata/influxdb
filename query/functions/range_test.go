package functions_test

import (
	"testing"
	"time"

	"github.com/influxdata/platform/query/functions"
	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/execute"
	"github.com/influxdata/platform/query/plan"
	"github.com/influxdata/platform/query/plan/plantest"
	"github.com/influxdata/platform/query/querytest"
)

func TestRange_NewQuery(t *testing.T) {
	tests := []querytest.NewQueryTestCase{
		{
			Name: "from with database with range",
			Raw:  `from(db:"mydb") |> range(start:-4h, stop:-2h) |> sum()`,
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
						ID: "sum2",
						Spec: &functions.SumOpSpec{
							AggregateConfig: execute.DefaultAggregateConfig,
						},
					},
				},
				Edges: []query.Edge{
					{Parent: "from0", Child: "range1"},
					{Parent: "range1", Child: "sum2"},
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

func TestRangeOperation_Marshaling(t *testing.T) {
	data := []byte(`{"id":"range","kind":"range","spec":{"start":"-1h","stop":"2017-10-10T00:00:00Z"}}`)
	op := &query.Operation{
		ID: "range",
		Spec: &functions.RangeOpSpec{
			Start: query.Time{
				Relative:   -1 * time.Hour,
				IsRelative: true,
			},
			Stop: query.Time{
				Absolute: time.Date(2017, 10, 10, 0, 0, 0, 0, time.UTC),
			},
		},
	}

	querytest.OperationMarshalingTestHelper(t, data, op)
}

func TestRange_PushDown(t *testing.T) {
	spec := &functions.RangeProcedureSpec{
		Bounds: plan.BoundsSpec{
			Stop: query.Now,
		},
	}
	root := &plan.Procedure{
		Spec: new(functions.FromProcedureSpec),
	}
	want := &plan.Procedure{
		Spec: &functions.FromProcedureSpec{
			BoundsSet: true,
			Bounds: plan.BoundsSpec{
				Stop: query.Now,
			},
		},
	}

	plantest.PhysicalPlan_PushDown_TestHelper(t, spec, root, false, want)
}
func TestRange_PushDown_Duplicate(t *testing.T) {
	spec := &functions.RangeProcedureSpec{
		Bounds: plan.BoundsSpec{
			Stop: query.Now,
		},
	}
	root := &plan.Procedure{
		Spec: &functions.FromProcedureSpec{
			BoundsSet: true,
			Bounds: plan.BoundsSpec{
				Start: query.MinTime,
				Stop:  query.Now,
			},
		},
	}
	want := &plan.Procedure{
		// Expect the duplicate has been reset to zero values
		Spec: new(functions.FromProcedureSpec),
	}

	plantest.PhysicalPlan_PushDown_TestHelper(t, spec, root, true, want)
}
