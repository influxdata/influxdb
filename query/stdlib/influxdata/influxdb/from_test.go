package influxdb_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/plan"
	"github.com/influxdata/flux/plan/plantest"
	"github.com/influxdata/flux/querytest"
	"github.com/influxdata/flux/stdlib/universe"
	platform "github.com/influxdata/influxdb/v2"
	pquerytest "github.com/influxdata/influxdb/v2/query/querytest"
	"github.com/influxdata/influxdb/v2/query/stdlib/influxdata/influxdb"
)

func TestFrom_NewQuery(t *testing.T) {
	t.Skip()
	tests := []querytest.NewQueryTestCase{
		{
			Name:    "from no args",
			Raw:     `from()`,
			WantErr: true,
		},
		{
			Name:    "from conflicting args",
			Raw:     `from(bucket:"d", bucket:"b")`,
			WantErr: true,
		},
		{
			Name:    "from repeat arg",
			Raw:     `from(bucket:"telegraf", bucket:"oops")`,
			WantErr: true,
		},
		{
			Name:    "from",
			Raw:     `from(bucket:"telegraf", chicken:"what is this?")`,
			WantErr: true,
		},
		{
			Name:    "from bucket invalid ID",
			Raw:     `from(bucketID:"invalid")`,
			WantErr: true,
		},
		{
			Name: "from bucket ID",
			Raw:  `from(bucketID:"aaaabbbbccccdddd")`,
			Want: &flux.Spec{
				Operations: []*flux.Operation{
					{
						ID: "from0",
						Spec: &influxdb.FromOpSpec{
							BucketID: "aaaabbbbccccdddd",
						},
					},
				},
			},
		},
		{
			Name: "from with database",
			Raw:  `from(bucket:"mybucket") |> range(start:-4h, stop:-2h) |> sum()`,
			Want: &flux.Spec{
				Operations: []*flux.Operation{
					{
						ID: "from0",
						Spec: &influxdb.FromOpSpec{
							Bucket: "mybucket",
						},
					},
					{
						ID: "range1",
						Spec: &universe.RangeOpSpec{
							Start: flux.Time{
								Relative:   -4 * time.Hour,
								IsRelative: true,
							},
							Stop: flux.Time{
								Relative:   -2 * time.Hour,
								IsRelative: true,
							},
							TimeColumn:  "_time",
							StartColumn: "_start",
							StopColumn:  "_stop",
						},
					},
					{
						ID: "sum2",
						Spec: &universe.SumOpSpec{
							AggregateConfig: execute.DefaultAggregateConfig,
						},
					},
				},
				Edges: []flux.Edge{
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

func TestFromOperation_Marshaling(t *testing.T) {
	t.Skip()
	data := []byte(`{"id":"from","kind":"from","spec":{"bucket":"mybucket"}}`)
	op := &flux.Operation{
		ID: "from",
		Spec: &influxdb.FromOpSpec{
			Bucket: "mybucket",
		},
	}
	querytest.OperationMarshalingTestHelper(t, data, op)
}

func TestFromOpSpec_BucketsAccessed(t *testing.T) {
	bucketName := "my_bucket"
	bucketIDString := "aaaabbbbccccdddd"
	bucketID, err := platform.IDFromString(bucketIDString)
	if err != nil {
		t.Fatal(err)
	}
	invalidID := platform.InvalidID()
	tests := []pquerytest.BucketsAccessedTestCase{
		{
			Name:             "From with bucket",
			Raw:              fmt.Sprintf(`from(bucket:"%s")`, bucketName),
			WantReadBuckets:  &[]platform.BucketFilter{{Name: &bucketName}},
			WantWriteBuckets: &[]platform.BucketFilter{},
		},
		{
			Name:             "From with bucketID",
			Raw:              fmt.Sprintf(`from(bucketID:"%s")`, bucketID),
			WantReadBuckets:  &[]platform.BucketFilter{{ID: bucketID}},
			WantWriteBuckets: &[]platform.BucketFilter{},
		},
		{
			Name:             "From invalid bucketID",
			Raw:              `from(bucketID:"invalid")`,
			WantReadBuckets:  &[]platform.BucketFilter{{ID: &invalidID}},
			WantWriteBuckets: &[]platform.BucketFilter{},
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()
			pquerytest.BucketsAccessedTestHelper(t, tc)
		})
	}
}

func TestFromValidation(t *testing.T) {
	spec := plantest.PlanSpec{
		// from |> group (cannot query an infinite time range)
		Nodes: []plan.Node{
			plan.CreateLogicalNode("from", &influxdb.FromProcedureSpec{
				Bucket: "my-bucket",
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
		influxdb.PushDownRangeRule{},
		influxdb.PushDownFilterRule{},
		influxdb.PushDownGroupRule{},
	))
	_, err := pp.Plan(ps)
	if err == nil {
		t.Error("Expected query with no call to range to fail physical planning")
	}
	want := `cannot submit unbounded read to "my-bucket"; try bounding 'from' with a call to 'range'`
	got := err.Error()
	if want != got {
		t.Errorf("unexpected error; -want/+got\n- %s\n+ %s", want, got)
	}
}
