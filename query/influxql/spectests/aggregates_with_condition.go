package spectests

import (
	"fmt"
	"time"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/ast"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/functions/inputs"
	"github.com/influxdata/flux/functions/transformations"
	"github.com/influxdata/flux/semantic"
	"github.com/influxdata/influxql"
)

func init() {
	RegisterFixture(
		AggregateTest(func(aggregate flux.Operation) (stmt string, spec *flux.Spec) {
			return fmt.Sprintf(`SELECT %s(value) FROM db0..cpu WHERE host = 'server01'`, aggregate.Spec.Kind()),
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
									Block: &semantic.FunctionBlock{
										Parameters: &semantic.FunctionParameters{
											List: []*semantic.FunctionParameter{
												{Key: &semantic.Identifier{Name: "r"}},
											},
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
						},
						{
							ID: "filter1",
							Spec: &transformations.FilterOpSpec{
								Fn: &semantic.FunctionExpression{
									Block: &semantic.FunctionBlock{
										Parameters: &semantic.FunctionParameters{
											List: []*semantic.FunctionParameter{
												{Key: &semantic.Identifier{Name: "r"}},
											},
										},
										Body: &semantic.BinaryExpression{
											Operator: ast.EqualOperator,
											Left: &semantic.MemberExpression{
												Object: &semantic.IdentifierExpression{
													Name: "r",
												},
												Property: "host",
											},
											Right: &semantic.StringLiteral{
												Value: "server01",
											},
										},
									},
								},
							},
						},
						{
							ID: "group0",
							Spec: &transformations.GroupOpSpec{
								By: []string{"_measurement", "_start"},
							},
						},
						&aggregate,
						{
							ID: "duplicate0",
							Spec: &transformations.DuplicateOpSpec{
								Col: execute.DefaultStartColLabel,
								As:  execute.DefaultTimeColLabel,
							},
						},
						{
							ID: "map0",
							Spec: &transformations.MapOpSpec{
								Fn: &semantic.FunctionExpression{
									Block: &semantic.FunctionBlock{
										Parameters: &semantic.FunctionParameters{
											List: []*semantic.FunctionParameter{{
												Key: &semantic.Identifier{Name: "r"},
											}},
										},
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
													Key: &semantic.Identifier{Name: string(aggregate.Spec.Kind())},
													Value: &semantic.MemberExpression{
														Object: &semantic.IdentifierExpression{
															Name: "r",
														},
														Property: "_value",
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
						{Parent: "filter0", Child: "filter1"},
						{Parent: "filter1", Child: "group0"},
						{Parent: "group0", Child: aggregate.ID},
						{Parent: aggregate.ID, Child: "duplicate0"},
						{Parent: "duplicate0", Child: "map0"},
						{Parent: "map0", Child: "yield0"},
					},
					Now: Now(),
				}
		}),
	)
}
