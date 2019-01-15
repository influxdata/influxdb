package spectests

import (
	"fmt"
	"math"
	"time"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/ast"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/semantic"
	"github.com/influxdata/flux/stdlib/influxdata/influxdb"
	"github.com/influxdata/flux/stdlib/universe"
)

func init() {
	RegisterFixture(
		AggregateTest(func(aggregate flux.Operation) (stmt string, spec *flux.Spec) {
			return fmt.Sprintf(`SELECT %s(value) FROM db0..cpu WHERE time >= now() - 10m GROUP BY time(1m)`, aggregate.Spec.Kind()),
				&flux.Spec{
					Operations: []*flux.Operation{
						{
							ID: "from0",
							Spec: &influxdb.FromOpSpec{
								BucketID: bucketID.String(),
							},
						},
						{
							ID: "range0",
							Spec: &universe.RangeOpSpec{
								Start:       flux.Time{Absolute: Now().Add(-10 * time.Minute)},
								Stop:        flux.Time{Absolute: Now()},
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
							ID: "window0",
							Spec: &universe.WindowOpSpec{
								Every:       flux.Duration(time.Minute),
								Period:      flux.Duration(time.Minute),
								TimeColumn:  execute.DefaultTimeColLabel,
								StartColumn: execute.DefaultStartColLabel,
								StopColumn:  execute.DefaultStopColLabel,
							},
						},
						&aggregate,
						{
							ID: "duplicate0",
							Spec: &universe.DuplicateOpSpec{
								Column: execute.DefaultStartColLabel,
								As:     execute.DefaultTimeColLabel,
							},
						},
						{
							ID: "window1",
							Spec: &universe.WindowOpSpec{
								Every:       flux.Duration(math.MaxInt64),
								Period:      flux.Duration(math.MaxInt64),
								TimeColumn:  execute.DefaultTimeColLabel,
								StartColumn: execute.DefaultStartColLabel,
								StopColumn:  execute.DefaultStopColLabel,
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
							Spec: &universe.YieldOpSpec{
								Name: "0",
							},
						},
					},
					Edges: []flux.Edge{
						{Parent: "from0", Child: "range0"},
						{Parent: "range0", Child: "filter0"},
						{Parent: "filter0", Child: "group0"},
						{Parent: "group0", Child: "window0"},
						{Parent: "window0", Child: aggregate.ID},
						{Parent: aggregate.ID, Child: "duplicate0"},
						{Parent: "duplicate0", Child: "window1"},
						{Parent: "window1", Child: "map0"},
						{Parent: "map0", Child: "yield0"},
					},
					Now: Now(),
				}
		}),
	)
}
