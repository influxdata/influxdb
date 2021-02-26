package influxdb_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/dependencies/dependenciestest"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/execute/executetest"
	"github.com/influxdata/flux/querytest"
	"github.com/influxdata/flux/runtime"
	_ "github.com/influxdata/flux/stdlib"
	"github.com/influxdata/influxdb/coordinator"
	"github.com/influxdata/influxdb/flux/stdlib/influxdata/influxdb"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/testing/assert"
	"github.com/influxdata/influxdb/services/meta"
)

func TestTo_Query(t *testing.T) {
	runtime.FinalizeBuiltIns()
	tests := []querytest.NewQueryTestCase{
		{
			Name: "from with database with range",
			Raw:  `from(bucket:"mydb") |> to(bucket:"myotherdb/autogen")`,
			Want: &flux.Spec{
				Operations: []*flux.Operation{
					{
						ID: "influxDBFrom0",
						Spec: &influxdb.FromOpSpec{
							Bucket: "mydb",
						},
					},
					{
						ID: "influx1x/toKind1",
						Spec: &influxdb.ToOpSpec{
							Bucket: "myotherdb/autogen",
						},
					},
				},
				Edges: []flux.Edge{
					{Parent: "influxDBFrom0", Child: "influx1x/toKind1"},
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

func TestTo_Process(t *testing.T) {
	type wanted struct {
		tables []*executetest.Table
		result *mockPointsWriter
	}
	testCases := []struct {
		name string
		spec *influxdb.ToProcedureSpec
		data []flux.Table
		want wanted
	}{
		{
			name: "default case",
			spec: &influxdb.ToProcedureSpec{
				Spec: &influxdb.ToOpSpec{
					Bucket: "my_db",
				},
			},
			data: []flux.Table{executetest.MustCopyTable(&executetest.Table{
				ColMeta: []flux.ColMeta{
					{Label: "_start", Type: flux.TTime},
					{Label: "_stop", Type: flux.TTime},
					{Label: "_time", Type: flux.TTime},
					{Label: "_measurement", Type: flux.TString},
					{Label: "_field", Type: flux.TString},
					{Label: "_value", Type: flux.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(0), execute.Time(100), execute.Time(11), "a", "_value", 2.0},
					{execute.Time(0), execute.Time(100), execute.Time(21), "a", "_value", 2.0},
					{execute.Time(0), execute.Time(100), execute.Time(21), "b", "_value", 1.0},
					{execute.Time(0), execute.Time(100), execute.Time(31), "a", "_value", 3.0},
					{execute.Time(0), execute.Time(100), execute.Time(41), "c", "_value", 4.0},
				},
			})},
			want: wanted{
				result: &mockPointsWriter{
					points: mockPoints(`a _value=2 11
a _value=2 21
b _value=1 21
a _value=3 31
c _value=4 41`),
					db: "my_db",
					rp: "autogen",
				},
				tables: []*executetest.Table{{
					ColMeta: []flux.ColMeta{
						{Label: "_start", Type: flux.TTime},
						{Label: "_stop", Type: flux.TTime},
						{Label: "_time", Type: flux.TTime},
						{Label: "_measurement", Type: flux.TString},
						{Label: "_field", Type: flux.TString},
						{Label: "_value", Type: flux.TFloat},
					},
					Data: [][]interface{}{
						{execute.Time(0), execute.Time(100), execute.Time(11), "a", "_value", 2.0},
						{execute.Time(0), execute.Time(100), execute.Time(21), "a", "_value", 2.0},
						{execute.Time(0), execute.Time(100), execute.Time(21), "b", "_value", 1.0},
						{execute.Time(0), execute.Time(100), execute.Time(31), "a", "_value", 3.0},
						{execute.Time(0), execute.Time(100), execute.Time(41), "c", "_value", 4.0},
					},
				}},
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			deps := influxdb.Dependencies{
				FluxDeps: dependenciestest.Default(),
				StorageDeps: influxdb.StorageDependencies{
					MetaClient: new(mockMetaClient),
					PointsWriter: &mockPointsWriter{
						db: tc.want.result.db,
						rp: tc.want.result.rp,
					},
				},
			}
			executetest.ProcessTestHelper(
				t,
				tc.data,
				tc.want.tables,
				nil,
				func(d execute.Dataset, c execute.TableBuilderCache) execute.Transformation {
					ctx := deps.Inject(context.Background())
					newT, err := influxdb.NewToTransformation(ctx, d, c, tc.spec, deps.StorageDeps)
					if err != nil {
						t.Error(err)
					}
					return newT
				},
			)
			pw := deps.StorageDeps.PointsWriter.(*mockPointsWriter)
			assert.Equal(t, len(tc.want.result.points), len(pw.points))

			gotStr := pointsToStr(pw.points)
			wantStr := pointsToStr(tc.want.result.points)

			assert.Equal(t, wantStr, gotStr)
		})
	}
}

type mockPointsWriter struct {
	points models.Points
	db     string
	rp     string
}

func (m *mockPointsWriter) WritePointsInto(request *coordinator.IntoWriteRequest) error {
	if m.db != request.Database {
		return fmt.Errorf("Wrong database - %s != %s", m.db, request.Database)
	}
	if m.rp != request.RetentionPolicy {
		return fmt.Errorf("Wrong retention policy - %s != %s", m.rp, request.RetentionPolicy)
	}
	m.points = append(m.points, request.Points...)
	return nil
}

type mockMetaClient struct {
}

func (m *mockMetaClient) Databases() []meta.DatabaseInfo {
	panic("mockMetaClient.Databases not implemented")
}

func (m *mockMetaClient) Database(name string) *meta.DatabaseInfo {
	return &meta.DatabaseInfo{
		Name:                   name,
		DefaultRetentionPolicy: "autogen",
		RetentionPolicies:      []meta.RetentionPolicyInfo{{Name: "autogen"}},
	}
}

func pointsToStr(points []models.Point) string {
	outStr := ""
	for _, x := range points {
		outStr += x.String() + "\n"
	}
	return outStr
}

func mockPoints(pointdata string) []models.Point {
	points, err := models.ParsePoints([]byte(pointdata))
	if err != nil {
		return nil
	}
	return points
}
