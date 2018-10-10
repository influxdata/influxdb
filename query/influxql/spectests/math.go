//+build todo

package spectests

import (
	"time"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/ast"
	"github.com/influxdata/flux/execute"

	"github.com/influxdata/flux/semantic"
	"github.com/influxdata/platform/query/influxql"
)

func init() {
	RegisterFixture(
		NewFixture(
			`SELECT a + b FROM db0..cpu`,
			&flux.Spec{
				Operations: []*flux.Operation{
					{
						ID: "from0",
						Spec: &inputs.FromOpSpec{
							BucketID: bucketID.String(),
						},
					},
					{
						ID: "range0",
						Spec: &transformations.RangeOpSpec{
							Start:    flux.Time{Absolute: time.Unix(0, influxql.MinTime)},
							Stop:     flux.Time{Absolute: time.Unix(0, influxql.MaxTime)},
							TimeCol:  execute.DefaultTimeColLabel,
							StartCol: execute.DefaultStartColLabel,
							StopCol:  execute.DefaultStopColLabel,
						},
					},
					{
						ID: "filter0",
						Spec: &transformations.FilterOpSpec{
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
											Value: "a",
										},
									},
								},
							},
						},
					},
					{
						ID: "from1",
						Spec: &inputs.FromOpSpec{
							BucketID: bucketID.String(),
						},
					},
					{
						ID: "range1",
						Spec: &transformations.RangeOpSpec{
							Start:    flux.Time{Absolute: time.Unix(0, influxql.MinTime)},
							Stop:     flux.Time{Absolute: time.Unix(0, influxql.MaxTime)},
							TimeCol:  execute.DefaultTimeColLabel,
							StartCol: execute.DefaultStartColLabel,
							StopCol:  execute.DefaultStopColLabel,
						},
					},
					{
						ID: "filter1",
						Spec: &transformations.FilterOpSpec{
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
											Value: "b",
										},
									},
								},
							},
						},
					},
					{
						ID: "join0",
						Spec: &transformations.JoinOpSpec{
							On: []string{"_measurement"},
							TableNames: map[flux.OperationID]string{
								"filter0": "t0",
								"filter1": "t1",
							},
						},
					},
					{
						ID: "group0",
						Spec: &transformations.GroupOpSpec{
							By: []string{"_measurement", "_start"},
						},
					},
					{
						ID: "map0",
						Spec: &transformations.MapOpSpec{
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
											Key: &semantic.Identifier{Name: "a_b"},
											Value: &semantic.BinaryExpression{
												Operator: ast.AdditionOperator,
												Left: &semantic.MemberExpression{
													Object: &semantic.IdentifierExpression{
														Name: "r",
													},
													Property: "val0",
												},
												Right: &semantic.MemberExpression{
													Object: &semantic.IdentifierExpression{
														Name: "r",
													},
													Property: "val1",
												},
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
						Spec: &transformations.YieldOpSpec{
							Name: "0",
						},
					},
				},
				Edges: []flux.Edge{
					{Parent: "from0", Child: "range0"},
					{Parent: "range0", Child: "filter0"},
					{Parent: "from1", Child: "range1"},
					{Parent: "range1", Child: "filter1"},
					{Parent: "filter0", Child: "join0"},
					{Parent: "filter1", Child: "join0"},
					{Parent: "join0", Child: "group0"},
					{Parent: "group0", Child: "map0"},
					{Parent: "map0", Child: "yield0"},
				},
				Now: Now(),
			},
		),
	)
}
