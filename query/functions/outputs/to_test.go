package outputs_test

import (
	"context"
	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/ast"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/execute/executetest"
	"github.com/influxdata/flux/functions/inputs"
	"github.com/influxdata/flux/semantic"
	"github.com/influxdata/platform"
	"github.com/influxdata/platform/mock"
	"github.com/influxdata/platform/models"
	_ "github.com/influxdata/platform/query/builtin"
	"github.com/influxdata/platform/query/functions/outputs"
	"github.com/influxdata/platform/query/querytest"
	"github.com/influxdata/platform/tsdb"
	"testing"
)

func TestTo_Query(t *testing.T) {
	tests := []querytest.BucketAwareQueryTestCase{
		{
			Name: "from with database with range",
			Raw:  `from(bucket:"mydb") |> to(bucket:"series1", org:"fred", host:"localhost", token:"auth-token", fieldFn: (r) => ({ col: r.col }) )`,
			Want: &flux.Spec{
				Operations: []*flux.Operation{
					{
						ID: "from0",
						Spec: &inputs.FromOpSpec{
							Bucket: "mydb",
						},
					},
					{
						ID: "to1",
						Spec: &outputs.ToOpSpec{
							Bucket:            "series1",
							Org:               "fred",
							Host:              "localhost",
							Token:             "auth-token",
							TimeColumn:        execute.DefaultTimeColLabel,
							MeasurementColumn: outputs.DefaultMeasurementColLabel,
							FieldFn: &semantic.FunctionExpression{
								Block: &semantic.FunctionBlock{
									Parameters: &semantic.FunctionParameters{
										List: []*semantic.FunctionParameter{
											{
												Key: &semantic.Identifier{Name: "r"},
											},
										},
									},
									Body: &semantic.ObjectExpression{
										Properties: []*semantic.Property{
											{
												Key: &semantic.Identifier{Name: "col"},
												Value: &semantic.MemberExpression{
													Object:   &semantic.IdentifierExpression{Name: "r"},
													Property: "col",
												},
											},
										},
									},
								},
							},
						},
					},
				},
				Edges: []flux.Edge{
					{Parent: "from0", Child: "to1"},
				},
			},
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()
			querytest.BucketAwareQueryTestHelper(t, tc)
		})
	}
}

func TestToOpSpec_BucketsAccessed(t *testing.T) {
	// TODO(adam) add this test back when BucketsAccessed is restored for the from function
	// https://github.com/influxdata/flux/issues/114
	t.Skip("https://github.com/influxdata/flux/issues/114")
	bucketName := "my_bucket"
	orgName := "my_org"
	tests := []querytest.BucketAwareQueryTestCase{
		{
			Name:             "from() with bucket and to with org and bucket",
			Raw:              `from(bucket:"my_bucket") |> to(bucket:"my_bucket", org:"my_org")`,
			WantReadBuckets:  &[]platform.BucketFilter{{Name: &bucketName}},
			WantWriteBuckets: &[]platform.BucketFilter{{Name: &bucketName, Organization: &orgName}},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()
			querytest.BucketAwareQueryTestHelper(t, tc)
		})
	}
}

func TestTo_Process(t *testing.T) {
	oid, _ := (mockOrgLookup{}).Lookup(context.Background(), "my-org")
	bid, _ := (mockBucketLookup{}).Lookup(oid, "my-bucket")
	type wanted struct {
		result *mock.PointsWriter
		tables []*executetest.Table
	}
	testCases := []struct {
		name string
		spec *outputs.ToProcedureSpec
		data []flux.Table
		want wanted
	}{
		{
			name: "default case",
			spec: &outputs.ToProcedureSpec{
				Spec: &outputs.ToOpSpec{
					Org:               "my-org",
					Bucket:            "my-bucket",
					TimeColumn:        "_time",
					MeasurementColumn: "_measurement",
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
				result: &mock.PointsWriter{
					Points: mockPoints(oid, bid, `a _value=2.0 11
a _value=2.0 21
b _value=1.0 21
a _value=3.0 31
c _value=4.0 41`),
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
		{
			name: "no _measurement with multiple tag columns",
			spec: &outputs.ToProcedureSpec{
				Spec: &outputs.ToOpSpec{
					Org:               "my-org",
					Bucket:            "my-bucket",
					TimeColumn:        "_time",
					MeasurementColumn: "tag1",
				},
			},
			data: []flux.Table{executetest.MustCopyTable(&executetest.Table{
				ColMeta: []flux.ColMeta{
					{Label: "_time", Type: flux.TTime},
					{Label: "tag1", Type: flux.TString},
					{Label: "tag2", Type: flux.TString},
					{Label: "_field", Type: flux.TString},
					{Label: "_value", Type: flux.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(11), "a", "aa", "_value", 2.0},
					{execute.Time(21), "a", "bb", "_value", 2.0},
					{execute.Time(21), "b", "cc", "_value", 1.0},
					{execute.Time(31), "a", "dd", "_value", 3.0},
					{execute.Time(41), "c", "ee", "_value", 4.0},
				},
			})},
			want: wanted{
				result: &mock.PointsWriter{
					Points: mockPoints(oid, bid, `a,tag2=aa _value=2.0 11
a,tag2=bb _value=2.0 21
b,tag2=cc _value=1.0 21
a,tag2=dd _value=3.0 31
c,tag2=ee _value=4.0 41`),
				},
				tables: []*executetest.Table{{
					ColMeta: []flux.ColMeta{
						{Label: "_time", Type: flux.TTime},
						{Label: "tag1", Type: flux.TString},
						{Label: "tag2", Type: flux.TString},
						{Label: "_field", Type: flux.TString},
						{Label: "_value", Type: flux.TFloat},
					},
					Data: [][]interface{}{
						{execute.Time(11), "a", "aa", "_value", 2.0},
						{execute.Time(21), "a", "bb", "_value", 2.0},
						{execute.Time(21), "b", "cc", "_value", 1.0},
						{execute.Time(31), "a", "dd", "_value", 3.0},
						{execute.Time(41), "c", "ee", "_value", 4.0},
					},
				}},
			},
		},
		{
			name: "explicit tags",
			spec: &outputs.ToProcedureSpec{
				Spec: &outputs.ToOpSpec{
					Org:               "my-org",
					Bucket:            "my-bucket",
					TimeColumn:        "_time",
					TagColumns:        []string{"tag1", "tag2"},
					MeasurementColumn: "_measurement",
				},
			},
			data: []flux.Table{executetest.MustCopyTable(&executetest.Table{
				ColMeta: []flux.ColMeta{
					{Label: "_time", Type: flux.TTime},
					{Label: "_measurement", Type: flux.TString},
					{Label: "_field", Type: flux.TString},
					{Label: "_value", Type: flux.TFloat},
					{Label: "tag1", Type: flux.TString},
					{Label: "tag2", Type: flux.TString},
				},
				Data: [][]interface{}{
					{execute.Time(11), "m", "_value", 2.0, "a", "aa"},
					{execute.Time(21), "m", "_value", 2.0, "a", "bb"},
					{execute.Time(21), "m", "_value", 1.0, "b", "cc"},
					{execute.Time(31), "m", "_value", 3.0, "a", "dd"},
					{execute.Time(41), "m", "_value", 4.0, "c", "ee"},
				},
			})},
			want: wanted{
				result: &mock.PointsWriter{
					Points: mockPoints(oid, bid, `m,tag1=a,tag2=aa _value=2.0 11
m,tag1=a,tag2=bb _value=2.0 21
m,tag1=b,tag2=cc _value=1.0 21
m,tag1=a,tag2=dd _value=3.0 31
m,tag1=c,tag2=ee _value=4.0 41`),
				},
				tables: []*executetest.Table{{
					ColMeta: []flux.ColMeta{
						{Label: "_time", Type: flux.TTime},
						{Label: "_measurement", Type: flux.TString},
						{Label: "_field", Type: flux.TString},
						{Label: "_value", Type: flux.TFloat},
						{Label: "tag1", Type: flux.TString},
						{Label: "tag2", Type: flux.TString},
					},
					Data: [][]interface{}{
						{execute.Time(11), "m", "_value", 2.0, "a", "aa"},
						{execute.Time(21), "m", "_value", 2.0, "a", "bb"},
						{execute.Time(21), "m", "_value", 1.0, "b", "cc"},
						{execute.Time(31), "m", "_value", 3.0, "a", "dd"},
						{execute.Time(41), "m", "_value", 4.0, "c", "ee"},
					},
				}},
			},
		},
		{
			name: "explicit field function",
			spec: &outputs.ToProcedureSpec{
				Spec: &outputs.ToOpSpec{
					Org:               "my-org",
					Bucket:            "my-bucket",
					TimeColumn:        "_time",
					MeasurementColumn: "_measurement",
					FieldFn: &semantic.FunctionExpression{
						Block: &semantic.FunctionBlock{
							Parameters: &semantic.FunctionParameters{
								List: []*semantic.FunctionParameter{
									{
										Key: &semantic.Identifier{Name: "r"},
									},
								},
							},
							Body: &semantic.ObjectExpression{
								Properties: []*semantic.Property{
									{
										Key: &semantic.Identifier{Name: "temperature"},
										Value: &semantic.MemberExpression{
											Object:   &semantic.IdentifierExpression{Name: "r"},
											Property: "temperature",
										},
									},
								},
							},
						},
					},
				},
			},
			data: []flux.Table{executetest.MustCopyTable(&executetest.Table{
				ColMeta: []flux.ColMeta{
					{Label: "_time", Type: flux.TTime},
					{Label: "_measurement", Type: flux.TString},
					{Label: "temperature", Type: flux.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(11), "a", 2.0},
					{execute.Time(21), "a", 2.0},
					{execute.Time(21), "b", 1.0},
					{execute.Time(31), "a", 3.0},
					{execute.Time(41), "c", 4.0},
				},
			})},
			want: wanted{
				result: &mock.PointsWriter{
					Points: mockPoints(oid, bid, `a temperature=2.0 11
a temperature=2.0 21
b temperature=1.0 21
a temperature=3.0 31
c temperature=4.0 41`),
				},
				tables: []*executetest.Table{{
					ColMeta: []flux.ColMeta{
						{Label: "_time", Type: flux.TTime},
						{Label: "_measurement", Type: flux.TString},
						{Label: "temperature", Type: flux.TFloat},
					},
					Data: [][]interface{}{
						{execute.Time(11), "a", 2.0},
						{execute.Time(21), "a", 2.0},
						{execute.Time(21), "b", 1.0},
						{execute.Time(31), "a", 3.0},
						{execute.Time(41), "c", 4.0},
					},
				}},
			},
		},
		{
			name: "infer tags from complex field function",
			spec: &outputs.ToProcedureSpec{
				Spec: &outputs.ToOpSpec{
					Org:               "my-org",
					Bucket:            "my-bucket",
					TimeColumn:        "_time",
					MeasurementColumn: "tag",
					FieldFn: &semantic.FunctionExpression{
						Block: &semantic.FunctionBlock{
							Parameters: &semantic.FunctionParameters{
								List: []*semantic.FunctionParameter{
									{
										Key: &semantic.Identifier{Name: "r"},
									},
								},
							},
							Body: &semantic.ObjectExpression{
								Properties: []*semantic.Property{
									{
										Key: &semantic.Identifier{Name: "day"},
										Value: &semantic.MemberExpression{
											Object:   &semantic.IdentifierExpression{Name: "r"},
											Property: "day",
										},
									},
									{
										Key: &semantic.Identifier{Name: "temperature"},
										Value: &semantic.MemberExpression{
											Object:   &semantic.IdentifierExpression{Name: "r"},
											Property: "temperature",
										},
									},
									{
										Key: &semantic.Identifier{Name: "humidity"},
										Value: &semantic.MemberExpression{
											Object:   &semantic.IdentifierExpression{Name: "r"},
											Property: "humidity",
										},
									},
									{
										Key: &semantic.Identifier{Name: "ratio"},
										Value: &semantic.BinaryExpression{
											Operator: ast.DivisionOperator,
											Left: &semantic.MemberExpression{
												Object:   &semantic.IdentifierExpression{Name: "r"},
												Property: "temperature",
											},
											Right: &semantic.MemberExpression{
												Object:   &semantic.IdentifierExpression{Name: "r"},
												Property: "humidity",
											},
										},
									},
								},
							},
						},
					},
				},
			},
			data: []flux.Table{executetest.MustCopyTable(&executetest.Table{
				ColMeta: []flux.ColMeta{
					{Label: "_time", Type: flux.TTime},
					{Label: "day", Type: flux.TString},
					{Label: "tag", Type: flux.TString},
					{Label: "temperature", Type: flux.TFloat},
					{Label: "humidity", Type: flux.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(11), "Monday", "a", 2.0, 1.0},
					{execute.Time(21), "Tuesday", "a", 2.0, 2.0},
					{execute.Time(21), "Wednesday", "b", 1.0, 4.0},
					{execute.Time(31), "Thursday", "a", 3.0, 3.0},
					{execute.Time(41), "Friday", "c", 4.0, 5.0},
				},
			})},
			want: wanted{
				result: &mock.PointsWriter{
					Points: mockPoints(oid, bid, `a day="Monday",humidity=1.0,ratio=2.0,temperature=2.0 11
a day="Tuesday",humidity=2.0,ratio=1.0,temperature=2.0 21
b day="Wednesday",humidity=4.0,ratio=0.25,temperature=1.0 21
a day="Thursday",humidity=3.0,ratio=1.0,temperature=3.0 31
c day="Friday",humidity=5.0,ratio=0.8,temperature=4.0 41`),
				},
				tables: []*executetest.Table{{
					ColMeta: []flux.ColMeta{
						{Label: "_time", Type: flux.TTime},
						{Label: "day", Type: flux.TString},
						{Label: "tag", Type: flux.TString},
						{Label: "temperature", Type: flux.TFloat},
						{Label: "humidity", Type: flux.TFloat},
					},
					Data: [][]interface{}{
						{execute.Time(11), "Monday", "a", 2.0, 1.0},
						{execute.Time(21), "Tuesday", "a", 2.0, 2.0},
						{execute.Time(21), "Wednesday", "b", 1.0, 4.0},
						{execute.Time(31), "Thursday", "a", 3.0, 3.0},
						{execute.Time(41), "Friday", "c", 4.0, 5.0},
					},
				}},
			},
		},
		{
			name: "explicit tag columns, multiple values in field function, and extra columns",
			spec: &outputs.ToProcedureSpec{
				Spec: &outputs.ToOpSpec{
					Org:               "my-org",
					Bucket:            "my-bucket",
					TimeColumn:        "_time",
					MeasurementColumn: "tag1",
					TagColumns:        []string{"tag2"},
					FieldFn: &semantic.FunctionExpression{
						Block: &semantic.FunctionBlock{
							Parameters: &semantic.FunctionParameters{
								List: []*semantic.FunctionParameter{
									{
										Key: &semantic.Identifier{Name: "r"},
									},
								},
							},
							Body: &semantic.ObjectExpression{
								Properties: []*semantic.Property{
									{
										Key: &semantic.Identifier{Name: "temperature"},
										Value: &semantic.MemberExpression{
											Object:   &semantic.IdentifierExpression{Name: "r"},
											Property: "temperature",
										},
									},
									{
										Key: &semantic.Identifier{Name: "humidity"},
										Value: &semantic.MemberExpression{
											Object:   &semantic.IdentifierExpression{Name: "r"},
											Property: "humidity",
										},
									},
								},
							},
						},
					},
				},
			},
			data: []flux.Table{executetest.MustCopyTable(&executetest.Table{
				ColMeta: []flux.ColMeta{
					{Label: "_start", Type: flux.TTime},
					{Label: "_stop", Type: flux.TTime},
					{Label: "_time", Type: flux.TTime},
					{Label: "tag1", Type: flux.TString},
					{Label: "tag2", Type: flux.TString},
					{Label: "other-string-column", Type: flux.TString},
					{Label: "temperature", Type: flux.TFloat},
					{Label: "humidity", Type: flux.TInt},
					{Label: "other-value-column", Type: flux.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(0), execute.Time(100), execute.Time(11), "a", "d", "misc", 2.0, int64(50), 1.0},
					{execute.Time(0), execute.Time(100), execute.Time(21), "a", "d", "misc", 2.0, int64(50), 1.0},
					{execute.Time(0), execute.Time(100), execute.Time(21), "b", "d", "misc", 1.0, int64(50), 1.0},
					{execute.Time(0), execute.Time(100), execute.Time(31), "a", "e", "misc", 3.0, int64(60), 1.0},
					{execute.Time(0), execute.Time(100), execute.Time(41), "c", "e", "misc", 4.0, int64(65), 1.0},
				},
			})},
			want: wanted{
				result: &mock.PointsWriter{
					Points: mockPoints(oid, bid, `a,tag2=d humidity=50i,temperature=2.0 11
a,tag2=d humidity=50i,temperature=2.0 21
b,tag2=d humidity=50i,temperature=1.0 21
a,tag2=e humidity=60i,temperature=3.0 31
c,tag2=e humidity=65i,temperature=4.0 41`),
				},
				tables: []*executetest.Table{{
					ColMeta: []flux.ColMeta{
						{Label: "_start", Type: flux.TTime},
						{Label: "_stop", Type: flux.TTime},
						{Label: "_time", Type: flux.TTime},
						{Label: "tag1", Type: flux.TString},
						{Label: "tag2", Type: flux.TString},
						{Label: "other-string-column", Type: flux.TString},
						{Label: "temperature", Type: flux.TFloat},
						{Label: "humidity", Type: flux.TInt},
						{Label: "other-value-column", Type: flux.TFloat},
					},
					Data: [][]interface{}{
						{execute.Time(0), execute.Time(100), execute.Time(11), "a", "d", "misc", 2.0, int64(50), 1.0},
						{execute.Time(0), execute.Time(100), execute.Time(21), "a", "d", "misc", 2.0, int64(50), 1.0},
						{execute.Time(0), execute.Time(100), execute.Time(21), "b", "d", "misc", 1.0, int64(50), 1.0},
						{execute.Time(0), execute.Time(100), execute.Time(31), "a", "e", "misc", 3.0, int64(60), 1.0},
						{execute.Time(0), execute.Time(100), execute.Time(41), "c", "e", "misc", 4.0, int64(65), 1.0},
					},
				}},
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			deps := mockDependencies()
			executetest.ProcessTestHelper(
				t,
				tc.data,
				tc.want.tables,
				nil,
				func(d execute.Dataset, c execute.TableBuilderCache) execute.Transformation {
					newT, _ := outputs.NewToTransformation(d, c, tc.spec, deps)
					return newT
				},
			)
			pw := deps.PointsWriter.(*mock.PointsWriter)
			if len(pw.Points) != len(tc.want.result.Points) {
				t.Errorf("Expected result values to have length of %d but got %d", len(tc.want.result.Points), len(pw.Points))
			}

			gotStr := pointsToStr(pw.Points)
			wantStr := pointsToStr(tc.want.result.Points)

			if !cmp.Equal(gotStr, wantStr) {

				t.Errorf("got other than expected %s", cmp.Diff(gotStr, wantStr))
			}
		})
	}
}

func mockDependencies() outputs.ToDependencies {
	return outputs.ToDependencies{
		BucketLookup:       mockBucketLookup{},
		OrganizationLookup: mockOrgLookup{},
		PointsWriter:       new(mock.PointsWriter),
	}
}

type mockBucketLookup struct{}

func (mockBucketLookup) Lookup(orgID platform.ID, name string) (platform.ID, bool) {
	if name == "my-bucket" {
		return platform.ID(1), true
	}
	return platform.InvalidID(), false
}

type mockOrgLookup struct{}

func (mockOrgLookup) Lookup(_ context.Context, name string) (platform.ID, bool) {
	if name == "my-org" {
		return platform.ID(2), true
	}
	return platform.InvalidID(), false
}

func pointsToStr(points []models.Point) string {
	outStr := ""
	for _, x := range points {
		outStr += x.String() + "\n"
	}
	return outStr
}

func mockPoints(org, bucket platform.ID, pointdata string) []models.Point {
	points, err := models.ParsePoints([]byte(pointdata))
	if err != nil {
		return nil
	}

	exploded, err := tsdb.ExplodePoints(org, bucket, points)
	if err != nil {
		return nil
	}

	return exploded
}
