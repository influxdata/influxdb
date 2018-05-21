package plan_test

import (
	"strconv"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/platform/query/functions"
	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/plan"
	"github.com/influxdata/platform/query/plan/plantest"
)

func TestLogicalPlanner_Plan(t *testing.T) {
	testCases := []struct {
		q  *query.Spec
		ap *plan.LogicalPlanSpec
	}{
		{
			q: &query.Spec{
				Operations: []*query.Operation{
					{
						ID: "0",
						Spec: &functions.FromOpSpec{
							Database: "mydb",
						},
					},
					{
						ID: "1",
						Spec: &functions.RangeOpSpec{
							Start: query.Time{Relative: -1 * time.Hour},
							Stop:  query.Time{},
						},
					},
					{
						ID:   "2",
						Spec: &functions.CountOpSpec{},
					},
				},
				Edges: []query.Edge{
					{Parent: "0", Child: "1"},
					{Parent: "1", Child: "2"},
				},
			},
			ap: &plan.LogicalPlanSpec{
				Procedures: map[plan.ProcedureID]*plan.Procedure{
					plan.ProcedureIDFromOperationID("0"): {
						ID: plan.ProcedureIDFromOperationID("0"),
						Spec: &functions.FromProcedureSpec{
							Database: "mydb",
						},
						Parents:  nil,
						Children: []plan.ProcedureID{plan.ProcedureIDFromOperationID("1")},
					},
					plan.ProcedureIDFromOperationID("1"): {
						ID: plan.ProcedureIDFromOperationID("1"),
						Spec: &functions.RangeProcedureSpec{
							Bounds: plan.BoundsSpec{
								Start: query.Time{Relative: -1 * time.Hour},
							},
						},
						Parents: []plan.ProcedureID{
							plan.ProcedureIDFromOperationID("0"),
						},
						Children: []plan.ProcedureID{plan.ProcedureIDFromOperationID("2")},
					},
					plan.ProcedureIDFromOperationID("2"): {
						ID:   plan.ProcedureIDFromOperationID("2"),
						Spec: &functions.CountProcedureSpec{},
						Parents: []plan.ProcedureID{
							plan.ProcedureIDFromOperationID("1"),
						},
						Children: nil,
					},
				},
				Order: []plan.ProcedureID{
					plan.ProcedureIDFromOperationID("0"),
					plan.ProcedureIDFromOperationID("1"),
					plan.ProcedureIDFromOperationID("2"),
				},
			},
		},
		{
			q: benchmarkQuery,
			ap: &plan.LogicalPlanSpec{
				Procedures: map[plan.ProcedureID]*plan.Procedure{
					plan.ProcedureIDFromOperationID("select0"): {
						ID: plan.ProcedureIDFromOperationID("select0"),
						Spec: &functions.FromProcedureSpec{
							Database: "mydb",
						},
						Parents:  nil,
						Children: []plan.ProcedureID{plan.ProcedureIDFromOperationID("range0")},
					},
					plan.ProcedureIDFromOperationID("range0"): {
						ID: plan.ProcedureIDFromOperationID("range0"),
						Spec: &functions.RangeProcedureSpec{
							Bounds: plan.BoundsSpec{
								Start: query.Time{Relative: -1 * time.Hour},
							},
						},
						Parents: []plan.ProcedureID{
							plan.ProcedureIDFromOperationID("select0"),
						},
						Children: []plan.ProcedureID{plan.ProcedureIDFromOperationID("count0")},
					},
					plan.ProcedureIDFromOperationID("count0"): {
						ID:   plan.ProcedureIDFromOperationID("count0"),
						Spec: &functions.CountProcedureSpec{},
						Parents: []plan.ProcedureID{
							plan.ProcedureIDFromOperationID("range0"),
						},
						Children: []plan.ProcedureID{plan.ProcedureIDFromOperationID("join")},
					},
					plan.ProcedureIDFromOperationID("select1"): {
						ID: plan.ProcedureIDFromOperationID("select1"),
						Spec: &functions.FromProcedureSpec{
							Database: "mydb",
						},
						Parents:  nil,
						Children: []plan.ProcedureID{plan.ProcedureIDFromOperationID("range1")},
					},
					plan.ProcedureIDFromOperationID("range1"): {
						ID: plan.ProcedureIDFromOperationID("range1"),
						Spec: &functions.RangeProcedureSpec{
							Bounds: plan.BoundsSpec{
								Start: query.Time{Relative: -1 * time.Hour},
							},
						},
						Parents: []plan.ProcedureID{
							plan.ProcedureIDFromOperationID("select1"),
						},
						Children: []plan.ProcedureID{plan.ProcedureIDFromOperationID("sum1")},
					},
					plan.ProcedureIDFromOperationID("sum1"): {
						ID:   plan.ProcedureIDFromOperationID("sum1"),
						Spec: &functions.SumProcedureSpec{},
						Parents: []plan.ProcedureID{
							plan.ProcedureIDFromOperationID("range1"),
						},
						Children: []plan.ProcedureID{plan.ProcedureIDFromOperationID("join")},
					},
					plan.ProcedureIDFromOperationID("join"): {
						ID: plan.ProcedureIDFromOperationID("join"),
						Spec: &functions.MergeJoinProcedureSpec{
							TableNames: map[plan.ProcedureID]string{
								plan.ProcedureIDFromOperationID("sum1"):   "sum",
								plan.ProcedureIDFromOperationID("count0"): "count",
							},
						},
						Parents: []plan.ProcedureID{
							plan.ProcedureIDFromOperationID("count0"),
							plan.ProcedureIDFromOperationID("sum1"),
						},
						Children: nil,
					},
				},
				Order: []plan.ProcedureID{
					plan.ProcedureIDFromOperationID("select1"),
					plan.ProcedureIDFromOperationID("range1"),
					plan.ProcedureIDFromOperationID("sum1"),
					plan.ProcedureIDFromOperationID("select0"),
					plan.ProcedureIDFromOperationID("range0"),
					plan.ProcedureIDFromOperationID("count0"),
					plan.ProcedureIDFromOperationID("join"),
				},
			},
		},
	}
	for i, tc := range testCases {
		tc := tc
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			planner := plan.NewLogicalPlanner()
			got, err := planner.Plan(tc.q)
			if err != nil {
				t.Fatal(err)
			}
			if !cmp.Equal(got, tc.ap, plantest.CmpOptions...) {
				t.Errorf("unexpected logical plan -want/+got %s", cmp.Diff(tc.ap, got, plantest.CmpOptions...))
			}
		})
	}
}

var benchmarkQuery = &query.Spec{
	Operations: []*query.Operation{
		{
			ID: "select0",
			Spec: &functions.FromOpSpec{
				Database: "mydb",
			},
		},
		{
			ID: "range0",
			Spec: &functions.RangeOpSpec{
				Start: query.Time{Relative: -1 * time.Hour},
				Stop:  query.Time{},
			},
		},
		{
			ID:   "count0",
			Spec: &functions.CountOpSpec{},
		},
		{
			ID: "select1",
			Spec: &functions.FromOpSpec{
				Database: "mydb",
			},
		},
		{
			ID: "range1",
			Spec: &functions.RangeOpSpec{
				Start: query.Time{Relative: -1 * time.Hour},
				Stop:  query.Time{},
			},
		},
		{
			ID:   "sum1",
			Spec: &functions.SumOpSpec{},
		},
		{
			ID: "join",
			Spec: &functions.JoinOpSpec{
				TableNames: map[query.OperationID]string{
					"count0": "count",
					"sum1":   "sum",
				},
			},
		},
	},
	Edges: []query.Edge{
		{Parent: "select0", Child: "range0"},
		{Parent: "range0", Child: "count0"},
		{Parent: "select1", Child: "range1"},
		{Parent: "range1", Child: "sum1"},
		{Parent: "count0", Child: "join"},
		{Parent: "sum1", Child: "join"},
	},
}

var benchLogicalPlan *plan.LogicalPlanSpec

func BenchmarkLogicalPlan(b *testing.B) {
	var err error
	planner := plan.NewLogicalPlanner()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		benchLogicalPlan, err = planner.Plan(benchmarkQuery)
		if err != nil {
			b.Fatal(err)
		}
	}
}
