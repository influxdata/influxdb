package experimental_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/execute/executetest"
	"github.com/influxdata/flux/querytest"
	fluxexperimental "github.com/influxdata/flux/stdlib/experimental"
	"github.com/influxdata/flux/stdlib/universe"
	_ "github.com/influxdata/influxdb/v2/fluxinit/static"
	platform2 "github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/query/stdlib/experimental"
	"github.com/influxdata/influxdb/v2/query/stdlib/influxdata/influxdb"
)

func TestTo_Query(t *testing.T) {
	tests := []querytest.NewQueryTestCase{
		{
			Name: "from range pivot experimental to local bucket",
			Raw: `import "experimental"
import "influxdata/influxdb/v1"
from(bucket:"mydb")
  |> range(start: -1h)
  |> v1.fieldsAsCols()
  |> experimental.to(bucket:"series1", org:"fred")`,
			Want: &flux.Spec{
				Operations: []*flux.Operation{
					{
						ID: "from0",
						Spec: &influxdb.FromOpSpec{
							Bucket: influxdb.NameOrID{Name: "mydb"},
						},
					},
					{
						ID: "range1",
						Spec: &universe.RangeOpSpec{
							Start:       flux.Time{IsRelative: true, Relative: -time.Hour},
							Stop:        flux.Time{IsRelative: true},
							TimeColumn:  "_time",
							StartColumn: "_start",
							StopColumn:  "_stop",
						},
					},
					{
						ID: "pivot2",
						Spec: &universe.PivotOpSpec{
							RowKey:      []string{"_time"},
							ColumnKey:   []string{"_field"},
							ValueColumn: "_value"},
					},
					{
						ID: "influxdb-experimental-to3",
						Spec: &experimental.ToOpSpec{
							Bucket: "series1",
							Org:    "fred",
						},
					},
				},
				Edges: []flux.Edge{
					{Parent: "from0", Child: "range1"},
					{Parent: "range1", Child: "pivot2"},
					{Parent: "pivot2", Child: "influxdb-experimental-to3"},
				},
			},
		},
		{
			Name: "from range pivot experimental to remote bucket",
			Raw: `import "experimental"
import "influxdata/influxdb/v1"
from(bucket:"mydb")
  |> range(start: -1h)
  |> v1.fieldsAsCols()
  |> experimental.to(bucket:"series1", org:"fred", host:"remote-host", token:"auth-token")`,
			Want: &flux.Spec{
				Operations: []*flux.Operation{
					{
						ID: "from0",
						Spec: &influxdb.FromOpSpec{
							Bucket: influxdb.NameOrID{Name: "mydb"},
						},
					},
					{
						ID: "range1",
						Spec: &universe.RangeOpSpec{
							Start:       flux.Time{IsRelative: true, Relative: -time.Hour},
							Stop:        flux.Time{IsRelative: true},
							TimeColumn:  "_time",
							StartColumn: "_start",
							StopColumn:  "_stop",
						},
					},
					{
						ID: "pivot2",
						Spec: &universe.PivotOpSpec{
							RowKey:      []string{"_time"},
							ColumnKey:   []string{"_field"},
							ValueColumn: "_value"},
					},
					{
						ID: "experimental-to3",
						Spec: &fluxexperimental.ToOpSpec{
							Bucket: influxdb.NameOrID{Name: "series1"},
							Org:    influxdb.NameOrID{Name: "fred"},
							Host:   "remote-host",
							Token:  "auth-token",
						},
					},
				},
				Edges: []flux.Edge{
					{Parent: "from0", Child: "range1"},
					{Parent: "range1", Child: "pivot2"},
					{Parent: "pivot2", Child: "experimental-to3"},
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
	oid, _ := mock.OrganizationLookup{}.Lookup(context.Background(), "my-org")
	bid, _ := mock.BucketLookup{}.Lookup(context.Background(), oid, "my-bucket")
	type wanted struct {
		result *mock.PointsWriter
	}
	testCases := []struct {
		name    string
		spec    *experimental.ToProcedureSpec
		data    []*executetest.Table
		want    wanted
		wantErr error
	}{
		{
			name: "measurement not in group key",
			spec: &experimental.ToProcedureSpec{
				Spec: &experimental.ToOpSpec{
					Org:    "my-org",
					Bucket: "my-bucket",
				},
			},
			data: []*executetest.Table{{
				KeyCols: []string{},
				ColMeta: []flux.ColMeta{
					{Label: "_start", Type: flux.TTime},
					{Label: "_stop", Type: flux.TTime},
					{Label: "_time", Type: flux.TTime},
					{Label: "_measurement", Type: flux.TString},
					{Label: "v", Type: flux.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(0), execute.Time(100), execute.Time(11), "a", 2.0},
					{execute.Time(0), execute.Time(100), execute.Time(21), "a", 2.0},
					{execute.Time(0), execute.Time(100), execute.Time(21), "b", 1.0},
					{execute.Time(0), execute.Time(100), execute.Time(31), "a", 3.0},
					{execute.Time(0), execute.Time(100), execute.Time(41), "c", 4.0},
				},
			}},
			wantErr: errors.New(`required column "_measurement" not in group key`),
		},
		{
			name: "non-string in group key",
			spec: &experimental.ToProcedureSpec{
				Spec: &experimental.ToOpSpec{
					Org:    "my-org",
					Bucket: "my-bucket",
				},
			},
			data: []*executetest.Table{{
				KeyCols: []string{"_measurement", "_start", "_stop", "gkcol"},
				ColMeta: []flux.ColMeta{
					{Label: "gkcol", Type: flux.TFloat},
					{Label: "_start", Type: flux.TTime},
					{Label: "_stop", Type: flux.TTime},
					{Label: "_time", Type: flux.TTime},
					{Label: "_measurement", Type: flux.TString},
					{Label: "v", Type: flux.TFloat},
				},
				Data: [][]interface{}{
					{100.0, execute.Time(0), execute.Time(100), execute.Time(11), "a", 2.0},
					{100.0, execute.Time(0), execute.Time(100), execute.Time(21), "a", 2.0},
					{100.0, execute.Time(0), execute.Time(100), execute.Time(21), "a", 1.0},
					{100.0, execute.Time(0), execute.Time(100), execute.Time(31), "a", 3.0},
					{100.0, execute.Time(0), execute.Time(100), execute.Time(41), "a", 4.0},
				},
			}},
			wantErr: errors.New(`group key column "gkcol" has type float; type string is required`),
		},
		{
			name: "unpivoted data with _field column",
			spec: &experimental.ToProcedureSpec{
				Spec: &experimental.ToOpSpec{
					Org:    "my-org",
					Bucket: "my-bucket",
				},
			},
			data: []*executetest.Table{{
				KeyCols: []string{"_measurement", "_start", "_stop", "_field"},
				ColMeta: []flux.ColMeta{
					{Label: "_field", Type: flux.TString},
					{Label: "_start", Type: flux.TTime},
					{Label: "_stop", Type: flux.TTime},
					{Label: "_time", Type: flux.TTime},
					{Label: "_measurement", Type: flux.TString},
					{Label: "_value", Type: flux.TFloat},
				},
				Data: [][]interface{}{
					{"cpu", execute.Time(0), execute.Time(100), execute.Time(11), "a", 2.0},
					{"cpu", execute.Time(0), execute.Time(100), execute.Time(21), "a", 2.0},
					{"cpu", execute.Time(0), execute.Time(100), execute.Time(21), "a", 1.0},
					{"cpu", execute.Time(0), execute.Time(100), execute.Time(31), "a", 3.0},
					{"cpu", execute.Time(0), execute.Time(100), execute.Time(41), "a", 4.0},
				},
			}},
			wantErr: errors.New(`found column "_field" in the group key; experimental.to() expects pivoted data`),
		},
		{
			name: "no time column",
			spec: &experimental.ToProcedureSpec{
				Spec: &experimental.ToOpSpec{
					Org:    "my-org",
					Bucket: "my-bucket",
				},
			},
			data: []*executetest.Table{{
				KeyCols: []string{"_measurement", "_start", "_stop"},
				ColMeta: []flux.ColMeta{
					{Label: "_start", Type: flux.TTime},
					{Label: "_stop", Type: flux.TTime},
					{Label: "_measurement", Type: flux.TString},
					{Label: "v", Type: flux.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(0), execute.Time(100), "a", 2.0},
					{execute.Time(0), execute.Time(100), "a", 2.0},
					{execute.Time(0), execute.Time(100), "a", 1.0},
					{execute.Time(0), execute.Time(100), "a", 1.0},
					{execute.Time(0), execute.Time(100), "a", 1.0},
					{execute.Time(0), execute.Time(100), "a", 3.0},
					{execute.Time(0), execute.Time(100), "a", 4.0},
				},
			}},
			wantErr: errors.New(`input table is missing required column "_time"`),
		},
		{
			name: "time column wrong type",
			spec: &experimental.ToProcedureSpec{
				Spec: &experimental.ToOpSpec{
					Org:    "my-org",
					Bucket: "my-bucket",
				},
			},
			data: []*executetest.Table{{
				KeyCols: []string{"_measurement", "_start", "_stop"},
				ColMeta: []flux.ColMeta{
					{Label: "_start", Type: flux.TTime},
					{Label: "_stop", Type: flux.TTime},
					{Label: "_time", Type: flux.TString},
					{Label: "_measurement", Type: flux.TString},
					{Label: "v", Type: flux.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(0), execute.Time(100), "eleven", "a", 2.0},
					{execute.Time(0), execute.Time(100), "twenty-one", "a", 2.0},
					{execute.Time(0), execute.Time(100), "twenty-one", "a", 1.0},
					{execute.Time(0), execute.Time(100), "thirty-one", "a", 3.0},
					{execute.Time(0), execute.Time(100), "forty-one", "a", 4.0},
				},
			}},
			wantErr: errors.New(`column "_time" has type string; type time is required`),
		},
		{
			name: "field invalid type",
			spec: &experimental.ToProcedureSpec{
				Spec: &experimental.ToOpSpec{
					Org:    "my-org",
					Bucket: "my-bucket",
				},
			},
			data: []*executetest.Table{{
				KeyCols: []string{"_measurement", "_start", "_stop"},
				ColMeta: []flux.ColMeta{
					{Label: "_start", Type: flux.TTime},
					{Label: "_stop", Type: flux.TTime},
					{Label: "_time", Type: flux.TTime},
					{Label: "_measurement", Type: flux.TString},
					{Label: "v", Type: flux.TTime},
				},
				Data: [][]interface{}{
					{execute.Time(0), execute.Time(100), execute.Time(11), "a", execute.Time(11)},
					{execute.Time(0), execute.Time(100), execute.Time(21), "a", execute.Time(11)},
					{execute.Time(0), execute.Time(100), execute.Time(21), "a", execute.Time(11)},
					{execute.Time(0), execute.Time(100), execute.Time(31), "a", execute.Time(11)},
					{execute.Time(0), execute.Time(100), execute.Time(41), "a", execute.Time(11)},
				},
			}},
			wantErr: errors.New("unsupported field type time"),
		},
		{
			name: "simple case",
			spec: &experimental.ToProcedureSpec{
				Spec: &experimental.ToOpSpec{
					Org:    "my-org",
					Bucket: "my-bucket",
				},
			},
			data: []*executetest.Table{{
				KeyCols: []string{"_measurement", "_start", "_stop"},
				ColMeta: []flux.ColMeta{
					{Label: "_start", Type: flux.TTime},
					{Label: "_stop", Type: flux.TTime},
					{Label: "_time", Type: flux.TTime},
					{Label: "_measurement", Type: flux.TString},
					{Label: "v", Type: flux.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(0), execute.Time(100), execute.Time(11), "a", 2.0},
					{execute.Time(0), execute.Time(100), execute.Time(21), "a", 2.0},
					{execute.Time(0), execute.Time(100), execute.Time(21), "a", 1.0},
					{execute.Time(0), execute.Time(100), execute.Time(31), "a", 3.0},
					{execute.Time(0), execute.Time(100), execute.Time(41), "a", 4.0},
				},
			}},
			want: wanted{
				result: &mock.PointsWriter{
					Points: mockPoints(oid, bid, `a v=2 11
a v=2 21
a v=1 21
a v=3 31
a v=4 41`),
				},
			},
		},
		{
			name: "two tags",
			spec: &experimental.ToProcedureSpec{
				Spec: &experimental.ToOpSpec{
					Org:    "my-org",
					Bucket: "my-bucket",
				},
			},
			data: []*executetest.Table{{
				KeyCols: []string{"_measurement", "_start", "_stop", "t"},
				ColMeta: []flux.ColMeta{
					{Label: "_start", Type: flux.TTime},
					{Label: "_stop", Type: flux.TTime},
					{Label: "_time", Type: flux.TTime},
					{Label: "_measurement", Type: flux.TString},
					{Label: "t", Type: flux.TString},
					{Label: "v", Type: flux.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(0), execute.Time(100), execute.Time(11), "a", "x", 2.0},
					{execute.Time(0), execute.Time(100), execute.Time(21), "a", "x", 2.0},
					{execute.Time(0), execute.Time(100), execute.Time(21), "a", "x", 1.0},
					{execute.Time(0), execute.Time(100), execute.Time(31), "a", "x", 3.0},
					{execute.Time(0), execute.Time(100), execute.Time(41), "a", "x", 4.0},
				},
			}},
			want: wanted{
				result: &mock.PointsWriter{
					Points: mockPoints(oid, bid, `a,t=x v=2 11
a,t=x v=2 21
a,t=x v=1 21
a,t=x v=3 31
a,t=x v=4 41`),
				},
			},
		},
		{
			name: "two tags measurement not first",
			spec: &experimental.ToProcedureSpec{
				Spec: &experimental.ToOpSpec{
					Org:    "my-org",
					Bucket: "my-bucket",
				},
			},
			data: []*executetest.Table{{
				KeyCols: []string{"_start", "_stop", "t", "_measurement"},
				ColMeta: []flux.ColMeta{
					{Label: "_start", Type: flux.TTime},
					{Label: "_stop", Type: flux.TTime},
					{Label: "_time", Type: flux.TTime},
					{Label: "t", Type: flux.TString},
					{Label: "_measurement", Type: flux.TString},
					{Label: "v", Type: flux.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(0), execute.Time(100), execute.Time(11), "x", "a", 2.0},
					{execute.Time(0), execute.Time(100), execute.Time(21), "x", "a", 2.0},
					{execute.Time(0), execute.Time(100), execute.Time(21), "x", "a", 1.0},
					{execute.Time(0), execute.Time(100), execute.Time(31), "x", "a", 3.0},
					{execute.Time(0), execute.Time(100), execute.Time(41), "x", "a", 4.0},
				},
			}},
			want: wanted{
				result: &mock.PointsWriter{
					Points: mockPoints(oid, bid, `a,t=x v=2 11
a,t=x v=2 21
a,t=x v=1 21
a,t=x v=3 31
a,t=x v=4 41`),
				},
			},
		},
		{
			name: "two fields",
			spec: &experimental.ToProcedureSpec{
				Spec: &experimental.ToOpSpec{
					Org:    "my-org",
					Bucket: "my-bucket",
				},
			},
			data: []*executetest.Table{{
				KeyCols: []string{"_measurement", "_start", "_stop"},
				ColMeta: []flux.ColMeta{
					{Label: "_start", Type: flux.TTime},
					{Label: "_stop", Type: flux.TTime},
					{Label: "_time", Type: flux.TTime},
					{Label: "_measurement", Type: flux.TString},
					{Label: "v", Type: flux.TFloat},
					{Label: "w", Type: flux.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(0), execute.Time(100), execute.Time(11), "a", 2.0, 3.5},
					{execute.Time(0), execute.Time(100), execute.Time(21), "a", 2.0, 3.5},
					{execute.Time(0), execute.Time(100), execute.Time(21), "a", 1.0, 2.5},
					{execute.Time(0), execute.Time(100), execute.Time(31), "a", 3.0, 4.5},
					{execute.Time(0), execute.Time(100), execute.Time(41), "a", 4.0, 5.5},
				},
			}},
			want: wanted{
				result: &mock.PointsWriter{
					Points: mockPoints(oid, bid, `a v=2,w=3.5 11
a v=2,w=3.5 21
a v=1,w=2.5 21
a v=3,w=4.5 31
a v=4,w=5.5 41`),
				},
			},
		},
		{
			name: "two fields and key column",
			spec: &experimental.ToProcedureSpec{
				Spec: &experimental.ToOpSpec{
					Org:    "my-org",
					Bucket: "my-bucket",
				},
			},
			data: []*executetest.Table{{
				KeyCols: []string{"_measurement", "key1"},
				ColMeta: []flux.ColMeta{
					{Label: "key1", Type: flux.TString},
					{Label: "_start", Type: flux.TTime},
					{Label: "_stop", Type: flux.TTime},
					{Label: "_time", Type: flux.TTime},
					{Label: "_measurement", Type: flux.TString},
					{Label: "v", Type: flux.TFloat},
					{Label: "w", Type: flux.TFloat},
				},
				Data: [][]interface{}{
					{"v1", execute.Time(0), execute.Time(100), execute.Time(11), "a", 2.0, 3.5},
					{"v1", execute.Time(0), execute.Time(100), execute.Time(21), "a", 2.0, 3.5},
					{"v1", execute.Time(0), execute.Time(100), execute.Time(21), "a", 1.0, 2.5},
					{"v1", execute.Time(0), execute.Time(100), execute.Time(31), "a", 3.0, 4.5},
					{"v1", execute.Time(0), execute.Time(100), execute.Time(41), "a", 4.0, 5.5},
				},
			}},
			want: wanted{
				result: &mock.PointsWriter{
					Points: mockPoints(oid, bid, `a,key1=v1 v=2,w=3.5 11
a,key1=v1 v=2,w=3.5 21
a,key1=v1 v=1,w=2.5 21
a,key1=v1 v=3,w=4.5 31
a,key1=v1 v=4,w=5.5 41`),
				},
			},
		},
		{
			name: "unordered tags",
			spec: &experimental.ToProcedureSpec{
				Spec: &experimental.ToOpSpec{
					Org:    "my-org",
					Bucket: "my-bucket",
				},
			},
			data: []*executetest.Table{{
				KeyCols: []string{"_measurement", "_start", "_stop", "t1", "t0"},
				ColMeta: []flux.ColMeta{
					{Label: "_start", Type: flux.TTime},
					{Label: "_stop", Type: flux.TTime},
					{Label: "_time", Type: flux.TTime},
					{Label: "_measurement", Type: flux.TString},
					{Label: "t1", Type: flux.TString},
					{Label: "t0", Type: flux.TString},
					{Label: "v", Type: flux.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(0), execute.Time(100), execute.Time(11), "a", "val1", "val0", 2.0},
					{execute.Time(0), execute.Time(100), execute.Time(21), "a", "val1", "val0", 2.0},
					{execute.Time(0), execute.Time(100), execute.Time(21), "a", "val1", "val0", 1.0},
					{execute.Time(0), execute.Time(100), execute.Time(31), "a", "val1", "val0", 3.0},
					{execute.Time(0), execute.Time(100), execute.Time(41), "a", "val1", "val0", 4.0},
				},
			}},
			want: wanted{
				result: &mock.PointsWriter{
					Points: mockPoints(oid, bid, `a,t0=val0,t1=val1 v=2 11
a,t0=val0,t1=val1 v=2 21
a,t0=val0,t1=val1 v=1 21
a,t0=val0,t1=val1 v=3 31
a,t0=val0,t1=val1 v=4 41`),
				},
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			deps := mockDependencies()
			inTables := make([]flux.Table, 0, len(tc.data))
			wantTables := make([]*executetest.Table, 0, len(tc.data))
			for _, tbl := range tc.data {
				rwTable := &executetest.RowWiseTable{Table: tbl}
				inTables = append(inTables, rwTable)
				wantTables = append(wantTables, tbl)
			}
			executetest.ProcessTestHelper(
				t,
				inTables,
				wantTables,
				tc.wantErr,
				func(d execute.Dataset, c execute.TableBuilderCache) execute.Transformation {
					newT, _ := experimental.NewToTransformation(context.TODO(), d, c, tc.spec, deps)
					return newT
				},
			)
			if tc.wantErr == nil {
				pw := deps.PointsWriter.(*mock.PointsWriter)
				if len(pw.Points) != len(tc.want.result.Points) {
					t.Errorf("Expected result values to have length of %d but got %d", len(tc.want.result.Points), len(pw.Points))
				}

				gotStr := pointsToStr(pw.Points)
				wantStr := pointsToStr(tc.want.result.Points)

				if !cmp.Equal(gotStr, wantStr) {
					t.Errorf("got other than expected %s", cmp.Diff(gotStr, wantStr))
				}
			}
		})
	}

}

func mockDependencies() influxdb.ToDependencies {
	return influxdb.ToDependencies{
		BucketLookup:       mock.BucketLookup{},
		OrganizationLookup: mock.OrganizationLookup{},
		PointsWriter:       new(mock.PointsWriter),
	}
}

func mockPoints(org, bucket platform2.ID, pointdata string) []models.Point {
	points, err := models.ParsePoints([]byte(pointdata))
	if err != nil {
		return nil
	}
	return points
}

func pointsToStr(points []models.Point) string {
	outStr := ""
	for _, x := range points {
		outStr += x.String() + "\n"
	}
	return outStr
}
