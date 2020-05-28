package influxdb_test

import (
	"context"
	"testing"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/plan"
	"github.com/influxdata/flux/plan/plantest"
	"github.com/influxdata/flux/stdlib/influxdata/influxdb"
	"github.com/influxdata/flux/stdlib/universe"
	qinfluxdb "github.com/influxdata/influxdb/v2/query/stdlib/influxdata/influxdb"
)

func TestFromValidation(t *testing.T) {
	spec := plantest.PlanSpec{
		// from |> group (cannot query an infinite time range)
		Nodes: []plan.Node{
			plan.CreateLogicalNode("from", &influxdb.FromProcedureSpec{
				Bucket: influxdb.NameOrID{Name: "my-bucket"},
			}),
			plan.CreatePhysicalNode("group", &universe.GroupProcedureSpec{
				GroupMode: flux.GroupModeBy,
				GroupKeys: []string{"_measurement", "_field"},
			}),
		},
		Edges: [][2]int{
			{0, 1},
		},
	}

	ps := plantest.CreatePlanSpec(&spec)
	pp := plan.NewPhysicalPlanner(plan.OnlyPhysicalRules(
		qinfluxdb.FromStorageRule{},
		qinfluxdb.PushDownRangeRule{},
		qinfluxdb.PushDownFilterRule{},
		qinfluxdb.PushDownGroupRule{},
	))
	_, err := pp.Plan(context.Background(), ps)
	if err == nil {
		t.Error("Expected query with no call to range to fail physical planning")
	}
	want := `cannot submit unbounded read to "my-bucket"; try bounding 'from' with a call to 'range'`
	got := err.Error()
	if want != got {
		t.Errorf("unexpected error; -want/+got\n- %s\n+ %s", want, got)
	}
}
