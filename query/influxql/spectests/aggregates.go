package spectests

import (
	"fmt"
	"path/filepath"
	"runtime"
	"time"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/ast"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/semantic"
	"github.com/influxdata/flux/stdlib/influxdata/influxdb"
	"github.com/influxdata/flux/stdlib/universe"
	"github.com/influxdata/influxql"
)

var aggregateCreateFuncs = []func(config execute.AggregateConfig) flux.OperationSpec{
	func(config execute.AggregateConfig) flux.OperationSpec {
		return &universe.CountOpSpec{AggregateConfig: config}
	},
	func(config execute.AggregateConfig) flux.OperationSpec {
		return &universe.MeanOpSpec{AggregateConfig: config}
	},
	func(config execute.AggregateConfig) flux.OperationSpec {
		return &universe.SumOpSpec{AggregateConfig: config}
	},
}

func AggregateTest(fn func(aggregate flux.Operation) (string, *flux.Spec)) Fixture {
	_, file, line, _ := runtime.Caller(1)
	fixture := &collection{
		file: filepath.Base(file),
		line: line,
	}

	for _, aggregateSpecFn := range aggregateCreateFuncs {
		spec := aggregateSpecFn(execute.AggregateConfig{
			Columns: []string{execute.DefaultValueColLabel},
		})
		op := flux.Operation{
			ID:   flux.OperationID(fmt.Sprintf("%s0", spec.Kind())),
			Spec: spec,
		}

		fixture.Add(fn(op))
	}
	return fixture
}

func init() {
	RegisterFixture(
		AggregateTest(func(aggregate flux.Operation) (stmt string, spec *flux.Spec) {
			return fmt.Sprintf(`SELECT %s(value) FROM db0..cpu`, aggregate.Spec.Kind()),
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
						&aggregate,
						{
							ID: "duplicate0",
							Spec: &universe.DuplicateOpSpec{
								Column: execute.DefaultStartColLabel,
								As:     execute.DefaultTimeColLabel,
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
