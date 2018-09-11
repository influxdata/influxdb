package spectests

import (
	"time"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/ast"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/functions"
	"github.com/influxdata/flux/semantic"
	"github.com/influxdata/influxql"
)

func init() {
	RegisterFixture(
		NewFixture(
			`SELECT mean(value), max(value) FROM db0..cpu`,
			&flux.Spec{
				Operations: []*flux.Operation{
					{
						ID: "from0",
						Spec: &functions.FromOpSpec{
							BucketID: bucketID,
						},
					},
					{
						ID: "range0",
						Spec: &functions.RangeOpSpec{
							Start:    flux.Time{Absolute: time.Unix(0, influxql.MinTime)},
							Stop:     flux.Time{Absolute: time.Unix(0, influxql.MaxTime)},
							TimeCol:  execute.DefaultTimeColLabel,
							StartCol: execute.DefaultStartColLabel,
							StopCol:  execute.DefaultStopColLabel,
						},
					},
					{
						ID: "filter0",
						Spec: &functions.FilterOpSpec{
							Fn: &semantic.FunctionExpression{
								Params: []*semantic.FunctionParam{
									{Key: &semantic.Identifier{Name: "r"}},
								},
								Body: &semantic.LogicalExpression{
									Operator: ast.AndOperator,
									Left: &semantic.BinaryExpression{
										Operator: ast.EqualOperator,
										Left: &semantic.MemberExpression{
											Object: &semantic.IdentifierExpression{
												Name: "r",
											},
											Property: "_measurement",
										},
										Right: &semantic.StringLiteral{
											Value: "cpu",
										},
									},
									Right: &semantic.BinaryExpression{
										Operator: ast.EqualOperator,
										Left: &semantic.MemberExpression{
											Object: &semantic.IdentifierExpression{
												Name: "r",
											},
											Property: "_field",
										},
										Right: &semantic.StringLiteral{
											Value: "value",
										},
									},
								},
							},
						},
					},
					{
						ID: "group0",
						Spec: &functions.GroupOpSpec{
							By: []string{"_measurement", "_start"},
						},
					},
					{
						ID: "mean0",
						Spec: &functions.MeanOpSpec{
							AggregateConfig: execute.AggregateConfig{
								Columns: []string{execute.DefaultValueColLabel},
							},
						},
					},
					{
						ID: "duplicate0",
						Spec: &functions.DuplicateOpSpec{
							Col: execute.DefaultStartColLabel,
							As:  execute.DefaultTimeColLabel,
						},
					},
					{
						ID: "from1",
						Spec: &functions.FromOpSpec{
							BucketID: bucketID,
						},
					},
					{
						ID: "range1",
						Spec: &functions.RangeOpSpec{
							Start:    flux.Time{Absolute: time.Unix(0, influxql.MinTime)},
							Stop:     flux.Time{Absolute: time.Unix(0, influxql.MaxTime)},
							TimeCol:  execute.DefaultTimeColLabel,
							StartCol: execute.DefaultStartColLabel,
							StopCol:  execute.DefaultStopColLabel,
						},
					},
					{
						ID: "filter1",
						Spec: &functions.FilterOpSpec{
							Fn: &semantic.FunctionExpression{
								Params: []*semantic.FunctionParam{
									{Key: &semantic.Identifier{Name: "r"}},
								},
								Body: &semantic.LogicalExpression{
									Operator: ast.AndOperator,
									Left: &semantic.BinaryExpression{
										Operator: ast.EqualOperator,
										Left: &semantic.MemberExpression{
											Object: &semantic.IdentifierExpression{
												Name: "r",
											},
											Property: "_measurement",
										},
										Right: &semantic.StringLiteral{
											Value: "cpu",
										},
									},
									Right: &semantic.BinaryExpression{
										Operator: ast.EqualOperator,
										Left: &semantic.MemberExpression{
											Object: &semantic.IdentifierExpression{
												Name: "r",
											},
											Property: "_field",
										},
										Right: &semantic.StringLiteral{
											Value: "value",
										},
									},
								},
							},
						},
					},
					{
						ID: "group1",
						Spec: &functions.GroupOpSpec{
							By: []string{"_measurement", "_start"},
						},
					},
					{
						ID: "max0",
						Spec: &functions.MaxOpSpec{
							SelectorConfig: execute.SelectorConfig{
								Column: execute.DefaultValueColLabel,
							},
						},
					},
					{
						ID: "drop0",
						Spec: &functions.DropOpSpec{
							Cols: []string{execute.DefaultTimeColLabel},
						},
					},
					{
						ID: "duplicate1",
						Spec: &functions.DuplicateOpSpec{
							Col: execute.DefaultStartColLabel,
							As:  execute.DefaultTimeColLabel,
						},
					},
					{
						ID: "join0",
						Spec: &functions.JoinOpSpec{
							On: []string{"_time", "_measurement"},
							TableNames: map[flux.OperationID]string{
								"duplicate0": "t0",
								"duplicate1": "t1",
							},
						},
					},
					{
						ID: "map0",
						Spec: &functions.MapOpSpec{
							Fn: &semantic.FunctionExpression{
								Params: []*semantic.FunctionParam{{
									Key: &semantic.Identifier{Name: "r"},
								}},
								Body: &semantic.ObjectExpression{
									Properties: []*semantic.Property{
										{
											Key: &semantic.Identifier{Name: "_time"},
											Value: &semantic.MemberExpression{
												Object: &semantic.IdentifierExpression{
													Name: "r",
												},
												Property: "_time",
											},
										},
										{
											Key: &semantic.Identifier{Name: "mean"},
											Value: &semantic.MemberExpression{
												Object: &semantic.IdentifierExpression{
													Name: "r",
												},
												Property: "t0__value",
											},
										},
										{
											Key: &semantic.Identifier{Name: "max"},
											Value: &semantic.MemberExpression{
												Object: &semantic.IdentifierExpression{
													Name: "r",
												},
												Property: "t1__value",
											},
										},
									},
								},
							},
							MergeKey: true,
						},
					},
					{
						ID: "yield0",
						Spec: &functions.YieldOpSpec{
							Name: "0",
						},
					},
				},
				Edges: []flux.Edge{
					{Parent: "from0", Child: "range0"},
					{Parent: "range0", Child: "filter0"},
					{Parent: "filter0", Child: "group0"},
					{Parent: "group0", Child: "mean0"},
					{Parent: "mean0", Child: "duplicate0"},
					{Parent: "from1", Child: "range1"},
					{Parent: "range1", Child: "filter1"},
					{Parent: "filter1", Child: "group1"},
					{Parent: "group1", Child: "max0"},
					{Parent: "max0", Child: "drop0"},
					{Parent: "drop0", Child: "duplicate1"},
					{Parent: "duplicate0", Child: "join0"},
					{Parent: "duplicate1", Child: "join0"},
					{Parent: "join0", Child: "map0"},
					{Parent: "map0", Child: "yield0"},
				},
				Now: Now(),
			},
		),
	)
}
