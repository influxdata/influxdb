package execute_test

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/platform"
	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/ast"
	_ "github.com/influxdata/platform/query/builtin"
	"github.com/influxdata/platform/query/execute"
	"github.com/influxdata/platform/query/execute/executetest"
	"github.com/influxdata/platform/query/functions"
	"github.com/influxdata/platform/query/plan"
	"github.com/influxdata/platform/query/semantic"
	uuid "github.com/satori/go.uuid"
)

var epoch = time.Unix(0, 0)
var orgID platform.ID

func init() {
	orgID.DecodeFromString("aaaa")
}

func TestExecutor_Execute(t *testing.T) {
	testCases := []struct {
		name string
		plan *plan.PlanSpec
		want map[string][]*executetest.Block
	}{
		{
			name: "simple aggregate",
			plan: &plan.PlanSpec{
				Now: epoch.Add(5),
				Resources: query.ResourceManagement{
					ConcurrencyQuota: 1,
					MemoryBytesQuota: math.MaxInt64,
				},
				Bounds: plan.BoundsSpec{
					Start: query.Time{Absolute: time.Unix(0, 1)},
					Stop:  query.Time{Absolute: time.Unix(0, 5)},
				},
				Procedures: map[plan.ProcedureID]*plan.Procedure{
					plan.ProcedureIDFromOperationID("from"): {
						ID: plan.ProcedureIDFromOperationID("from"),
						Spec: &testFromProcedureSource{
							data: []query.Block{&executetest.Block{
								KeyCols: []string{"_start", "_stop"},
								ColMeta: []query.ColMeta{
									{Label: "_start", Type: query.TTime},
									{Label: "_stop", Type: query.TTime},
									{Label: "_time", Type: query.TTime},
									{Label: "_value", Type: query.TFloat},
								},
								Data: [][]interface{}{
									{execute.Time(0), execute.Time(5), execute.Time(0), 1.0},
									{execute.Time(0), execute.Time(5), execute.Time(1), 2.0},
									{execute.Time(0), execute.Time(5), execute.Time(2), 3.0},
									{execute.Time(0), execute.Time(5), execute.Time(3), 4.0},
									{execute.Time(0), execute.Time(5), execute.Time(4), 5.0},
								},
							}},
						},
						Parents:  nil,
						Children: []plan.ProcedureID{plan.ProcedureIDFromOperationID("sum")},
					},
					plan.ProcedureIDFromOperationID("sum"): {
						ID: plan.ProcedureIDFromOperationID("sum"),
						Spec: &functions.SumProcedureSpec{
							AggregateConfig: execute.DefaultAggregateConfig,
						},
						Parents: []plan.ProcedureID{
							plan.ProcedureIDFromOperationID("from"),
						},
						Children: nil,
					},
				},
				Results: map[string]plan.YieldSpec{
					plan.DefaultYieldName: {ID: plan.ProcedureIDFromOperationID("sum")},
				},
			},
			want: map[string][]*executetest.Block{
				plan.DefaultYieldName: []*executetest.Block{{
					KeyCols: []string{"_start", "_stop"},
					ColMeta: []query.ColMeta{
						{Label: "_start", Type: query.TTime},
						{Label: "_stop", Type: query.TTime},
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
					},
					Data: [][]interface{}{
						{execute.Time(0), execute.Time(5), execute.Time(5), 15.0},
					},
				}},
			},
		},
		{
			name: "simple join",
			plan: &plan.PlanSpec{
				Now: epoch.Add(5),
				Resources: query.ResourceManagement{
					ConcurrencyQuota: 1,
					MemoryBytesQuota: math.MaxInt64,
				},
				Bounds: plan.BoundsSpec{
					Start: query.Time{Absolute: time.Unix(0, 1)},
					Stop:  query.Time{Absolute: time.Unix(0, 5)},
				},
				Procedures: map[plan.ProcedureID]*plan.Procedure{
					plan.ProcedureIDFromOperationID("from"): {
						ID: plan.ProcedureIDFromOperationID("from"),
						Spec: &testFromProcedureSource{
							data: []query.Block{&executetest.Block{
								KeyCols: []string{"_start", "_stop"},
								ColMeta: []query.ColMeta{
									{Label: "_start", Type: query.TTime},
									{Label: "_stop", Type: query.TTime},
									{Label: "_time", Type: query.TTime},
									{Label: "_value", Type: query.TInt},
								},
								Data: [][]interface{}{
									{execute.Time(0), execute.Time(5), execute.Time(0), int64(1)},
									{execute.Time(0), execute.Time(5), execute.Time(1), int64(2)},
									{execute.Time(0), execute.Time(5), execute.Time(2), int64(3)},
									{execute.Time(0), execute.Time(5), execute.Time(3), int64(4)},
									{execute.Time(0), execute.Time(5), execute.Time(4), int64(5)},
								},
							}},
						},
						Parents:  nil,
						Children: []plan.ProcedureID{plan.ProcedureIDFromOperationID("sum")},
					},
					plan.ProcedureIDFromOperationID("sum"): {
						ID: plan.ProcedureIDFromOperationID("sum"),
						Spec: &functions.SumProcedureSpec{
							AggregateConfig: execute.DefaultAggregateConfig,
						},
						Parents: []plan.ProcedureID{
							plan.ProcedureIDFromOperationID("from"),
						},
						Children: []plan.ProcedureID{plan.ProcedureIDFromOperationID("join")},
					},
					plan.ProcedureIDFromOperationID("count"): {
						ID: plan.ProcedureIDFromOperationID("count"),
						Spec: &functions.CountProcedureSpec{
							AggregateConfig: execute.DefaultAggregateConfig,
						},
						Parents: []plan.ProcedureID{
							plan.ProcedureIDFromOperationID("from"),
						},
						Children: []plan.ProcedureID{plan.ProcedureIDFromOperationID("join")},
					},
					plan.ProcedureIDFromOperationID("join"): {
						ID: plan.ProcedureIDFromOperationID("join"),
						Spec: &functions.MergeJoinProcedureSpec{
							TableNames: map[plan.ProcedureID]string{
								plan.ProcedureIDFromOperationID("sum"):   "sum",
								plan.ProcedureIDFromOperationID("count"): "count",
							},
							On: []string{"_time", "_start", "_stop"},
							Fn: &semantic.FunctionExpression{
								Params: []*semantic.FunctionParam{{Key: &semantic.Identifier{Name: "t"}}},
								Body: &semantic.ObjectExpression{
									Properties: []*semantic.Property{
										{
											Key: &semantic.Identifier{Name: "_time"},
											Value: &semantic.MemberExpression{
												Object: &semantic.MemberExpression{
													Object: &semantic.IdentifierExpression{
														Name: "t",
													},
													Property: "sum",
												},
												Property: "_time",
											},
										},
										{
											Key: &semantic.Identifier{Name: "_start"},
											Value: &semantic.MemberExpression{
												Object: &semantic.MemberExpression{
													Object: &semantic.IdentifierExpression{
														Name: "t",
													},
													Property: "sum",
												},
												Property: "_start",
											},
										},
										{
											Key: &semantic.Identifier{Name: "_stop"},
											Value: &semantic.MemberExpression{
												Object: &semantic.MemberExpression{
													Object: &semantic.IdentifierExpression{
														Name: "t",
													},
													Property: "sum",
												},
												Property: "_stop",
											},
										},
										{
											Key: &semantic.Identifier{Name: "_value"},
											Value: &semantic.BinaryExpression{
												Operator: ast.DivisionOperator,
												Left: &semantic.MemberExpression{
													Object: &semantic.MemberExpression{
														Object: &semantic.IdentifierExpression{
															Name: "t",
														},
														Property: "sum",
													},
													Property: "_value",
												},
												Right: &semantic.MemberExpression{
													Object: &semantic.MemberExpression{
														Object: &semantic.IdentifierExpression{
															Name: "t",
														},
														Property: "count",
													},
													Property: "_value",
												},
											},
										},
									},
								},
							},
						},
						Parents: []plan.ProcedureID{
							plan.ProcedureIDFromOperationID("sum"),
							plan.ProcedureIDFromOperationID("count"),
						},
						Children: nil,
					},
				},
				Results: map[string]plan.YieldSpec{
					plan.DefaultYieldName: {ID: plan.ProcedureIDFromOperationID("join")},
				},
			},
			want: map[string][]*executetest.Block{
				plan.DefaultYieldName: []*executetest.Block{{
					KeyCols: []string{"_start", "_stop"},
					ColMeta: []query.ColMeta{
						{Label: "_start", Type: query.TTime},
						{Label: "_stop", Type: query.TTime},
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TInt},
					},
					Data: [][]interface{}{
						{execute.Time(0), execute.Time(5), execute.Time(5), int64(3)},
					},
				}},
			},
		},
		{
			name: "multiple aggregates",
			plan: &plan.PlanSpec{
				Now: epoch.Add(5),
				Resources: query.ResourceManagement{
					ConcurrencyQuota: 1,
					MemoryBytesQuota: math.MaxInt64,
				},
				Bounds: plan.BoundsSpec{
					Start: query.Time{Absolute: time.Unix(0, 1)},
					Stop:  query.Time{Absolute: time.Unix(0, 5)},
				},
				Procedures: map[plan.ProcedureID]*plan.Procedure{
					plan.ProcedureIDFromOperationID("from"): {
						ID: plan.ProcedureIDFromOperationID("from"),
						Spec: &testFromProcedureSource{
							data: []query.Block{&executetest.Block{
								KeyCols: []string{"_start", "_stop"},
								ColMeta: []query.ColMeta{
									{Label: "_start", Type: query.TTime},
									{Label: "_stop", Type: query.TTime},
									{Label: "_time", Type: query.TTime},
									{Label: "_value", Type: query.TFloat},
								},
								Data: [][]interface{}{
									{execute.Time(0), execute.Time(5), execute.Time(0), 1.0},
									{execute.Time(0), execute.Time(5), execute.Time(1), 2.0},
									{execute.Time(0), execute.Time(5), execute.Time(2), 3.0},
									{execute.Time(0), execute.Time(5), execute.Time(3), 4.0},
									{execute.Time(0), execute.Time(5), execute.Time(4), 5.0},
								},
							}},
						},
						Parents: nil,
						Children: []plan.ProcedureID{
							plan.ProcedureIDFromOperationID("sum"),
							plan.ProcedureIDFromOperationID("mean"),
						},
					},
					plan.ProcedureIDFromOperationID("sum"): {
						ID: plan.ProcedureIDFromOperationID("sum"),
						Spec: &functions.SumProcedureSpec{
							AggregateConfig: execute.DefaultAggregateConfig,
						},
						Parents: []plan.ProcedureID{
							plan.ProcedureIDFromOperationID("from"),
						},
						Children: nil,
					},
					plan.ProcedureIDFromOperationID("mean"): {
						ID: plan.ProcedureIDFromOperationID("mean"),
						Spec: &functions.MeanProcedureSpec{
							AggregateConfig: execute.DefaultAggregateConfig,
						},
						Parents: []plan.ProcedureID{
							plan.ProcedureIDFromOperationID("from"),
						},
						Children: nil,
					},
				},
				Results: map[string]plan.YieldSpec{
					"sum":  {ID: plan.ProcedureIDFromOperationID("sum")},
					"mean": {ID: plan.ProcedureIDFromOperationID("mean")},
				},
			},
			want: map[string][]*executetest.Block{
				"sum": []*executetest.Block{{
					KeyCols: []string{"_start", "_stop"},
					ColMeta: []query.ColMeta{
						{Label: "_start", Type: query.TTime},
						{Label: "_stop", Type: query.TTime},
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
					},
					Data: [][]interface{}{
						{execute.Time(0), execute.Time(5), execute.Time(5), 15.0},
					},
				}},
				"mean": []*executetest.Block{{
					KeyCols: []string{"_start", "_stop"},
					ColMeta: []query.ColMeta{
						{Label: "_start", Type: query.TTime},
						{Label: "_stop", Type: query.TTime},
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
					},
					Data: [][]interface{}{
						{execute.Time(0), execute.Time(5), execute.Time(5), 3.0},
					},
				}},
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			exe := execute.NewExecutor(nil)
			results, err := exe.Execute(context.Background(), orgID, tc.plan)
			if err != nil {
				t.Fatal(err)
			}
			got := make(map[string][]*executetest.Block, len(results))
			for name, r := range results {
				if err := r.Blocks().Do(func(b query.Block) error {
					cb, err := executetest.ConvertBlock(b)
					if err != nil {
						return err
					}
					got[name] = append(got[name], cb)
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			}

			for _, g := range got {
				executetest.NormalizeBlocks(g)
			}
			for _, w := range tc.want {
				executetest.NormalizeBlocks(w)
			}

			if !cmp.Equal(got, tc.want) {
				t.Error("unexpected results -want/+got", cmp.Diff(tc.want, got))
			}
		})
	}
}

type testFromProcedureSource struct {
	data []query.Block
	ts   []execute.Transformation
}

func (p *testFromProcedureSource) Kind() plan.ProcedureKind {
	return "from-test"
}

func (p *testFromProcedureSource) Copy() plan.ProcedureSpec {
	return p
}

func (p *testFromProcedureSource) AddTransformation(t execute.Transformation) {
	p.ts = append(p.ts, t)
}

func (p *testFromProcedureSource) Run(ctx context.Context) {
	id := execute.DatasetID(uuid.NewV4())
	for _, t := range p.ts {
		var max execute.Time
		for _, b := range p.data {
			t.Process(id, b)
			stopIdx := execute.ColIdx(execute.DefaultStopColLabel, b.Cols())
			if stopIdx >= 0 {
				if s := b.Key().ValueTime(stopIdx); s > max {
					max = s
				}
			}
		}
		t.UpdateWatermark(id, max)
		t.Finish(id, nil)
	}
}

func init() {
	execute.RegisterSource("from-test", createTestFromSource)
}

func createTestFromSource(prSpec plan.ProcedureSpec, id execute.DatasetID, a execute.Administration) (execute.Source, error) {
	return prSpec.(*testFromProcedureSource), nil
}
