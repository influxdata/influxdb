package spectests

import (
	"time"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/ast"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/semantic"
	"github.com/influxdata/flux/stdlib/influxdata/influxdb"
	"github.com/influxdata/flux/stdlib/universe"
	"github.com/influxdata/influxql"
)

func init() {
	RegisterFixture(
		NewFixture(
			`SELECT value FROM db0.alternate.cpu`,
			&flux.Spec{
				Operations: []*flux.Operation{
					{
						ID: "from0",
						Spec: &influxdb.FromOpSpec{
							BucketID: altBucketID.String(),
						},
					},
					{
						ID: "range0",
						Spec: &universe.RangeOpSpec{
							Start:       flux.Time{Absolute: time.Unix(0, influxql.MinTime)},
							Stop:        flux.Time{Absolute: time.Unix(0, influxql.MaxTime)},
							TimeColumn:  execute.DefaultTimeColLabel,
							StartColumn: execute.DefaultStartColLabel,
							StopColumn:  execute.DefaultStopColLabel,
						},
					},
					{
						ID: "filter0",
						Spec: &universe.FilterOpSpec{
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
						ID: "group0",
						Spec: &universe.GroupOpSpec{
							Columns: []string{"_measurement", "_start"},
							Mode:    "by",
						},
					},
					{
						ID: "map0",
						Spec: &universe.MapOpSpec{
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
												Key: &semantic.Identifier{Name: "value"},
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
						Spec: &universe.YieldOpSpec{
							Name: "0",
						},
					},
				},
				Edges: []flux.Edge{
					{Parent: "from0", Child: "range0"},
					{Parent: "range0", Child: "filter0"},
					{Parent: "filter0", Child: "group0"},
					{Parent: "group0", Child: "map0"},
					{Parent: "map0", Child: "yield0"},
				},
				Now: Now(),
			},
		),
	)
}
