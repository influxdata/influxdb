package spectests

import (
	"time"

	"github.com/influxdata/influxql"
	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/ast"
	"github.com/influxdata/platform/query/functions"
	"github.com/influxdata/platform/query/semantic"
)

func init() {
	RegisterFixture(
		NewFixture(
			`SELECT a + b FROM db0..cpu`,
			&query.Spec{
				Operations: []*query.Operation{
					{
						ID: "from0",
						Spec: &functions.FromOpSpec{
							BucketID: bucketID,
						},
					},
					{
						ID: "range0",
						Spec: &functions.RangeOpSpec{
							Start: query.Time{Absolute: time.Unix(0, influxql.MinTime)},
							Stop:  query.Time{Absolute: time.Unix(0, influxql.MaxTime)},
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
											Value: "a",
										},
									},
								},
							},
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
							Start: query.Time{Absolute: time.Unix(0, influxql.MinTime)},
							Stop:  query.Time{Absolute: time.Unix(0, influxql.MaxTime)},
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
											Value: "b",
										},
									},
								},
							},
						},
					},
					{
						ID: "join0",
						Spec: &functions.JoinOpSpec{
							On: []string{"_measurement"},
							Fn: &semantic.FunctionExpression{
								Params: []*semantic.FunctionParam{{
									Key: &semantic.Identifier{Name: "tables"},
								}},
								Body: &semantic.ObjectExpression{
									Properties: []*semantic.Property{
										{
											Key: &semantic.Identifier{Name: "val0"},
											Value: &semantic.MemberExpression{
												Object: &semantic.IdentifierExpression{
													Name: "tables",
												},
												Property: "t0",
											},
										},
										{
											Key: &semantic.Identifier{Name: "val1"},
											Value: &semantic.MemberExpression{
												Object: &semantic.IdentifierExpression{
													Name: "tables",
												},
												Property: "t1",
											},
										},
									},
								},
							},
							TableNames: map[query.OperationID]string{
								"filter0": "t0",
								"filter1": "t1",
							},
						},
					},
					{
						ID: "group0",
						Spec: &functions.GroupOpSpec{
							By: []string{"_measurement"},
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
						Spec: &functions.YieldOpSpec{
							Name: "0",
						},
					},
				},
				Edges: []query.Edge{
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
