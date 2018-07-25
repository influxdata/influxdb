package spectests

import (
	"fmt"
	"math"
	"time"

	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/ast"
	"github.com/influxdata/platform/query/execute"
	"github.com/influxdata/platform/query/functions"
	"github.com/influxdata/platform/query/semantic"
)

func init() {
	RegisterFixture(
		AggregateTest(func(aggregate query.Operation) (stmt string, spec *query.Spec) {
			return fmt.Sprintf(`SELECT %s(value) FROM db0..cpu WHERE time >= now() - 10m GROUP BY time(1m)`, aggregate.Spec.Kind()),
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
								Start: query.Time{Absolute: Now().Add(-10 * time.Minute)},
								Stop:  query.Time{Absolute: Now()},
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
							ID: "window0",
							Spec: &functions.WindowOpSpec{
								Every:              query.Duration(time.Minute),
								Period:             query.Duration(time.Minute),
								IgnoreGlobalBounds: true,
								TimeCol:            execute.DefaultTimeColLabel,
								StartColLabel:      execute.DefaultStartColLabel,
								StopColLabel:       execute.DefaultStopColLabel,
							},
						},
						&aggregate,
						{
							ID: "window1",
							Spec: &functions.WindowOpSpec{
								Every:              query.Duration(math.MaxInt64),
								Period:             query.Duration(math.MaxInt64),
								IgnoreGlobalBounds: true,
								TimeCol:            execute.DefaultTimeColLabel,
								StartColLabel:      execute.DefaultStartColLabel,
								StopColLabel:       execute.DefaultStopColLabel,
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
						{Parent: "filter0", Child: "group0"},
						{Parent: "group0", Child: "window0"},
						{Parent: "window0", Child: aggregate.ID},
						{Parent: aggregate.ID, Child: "window1"},
						{Parent: "window1", Child: "map0"},
						{Parent: "map0", Child: "yield0"},
					},
					Now: Now(),
				}
		}),
	)
}
