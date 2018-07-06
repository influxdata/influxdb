package spectests

import (
	"fmt"
	"path/filepath"
	"runtime"
	"time"

	"github.com/influxdata/influxql"
	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/ast"
	"github.com/influxdata/platform/query/execute"
	"github.com/influxdata/platform/query/functions"
	"github.com/influxdata/platform/query/semantic"
)

var selectorCreateFuncs = []func(config execute.SelectorConfig) query.OperationSpec{
	func(config execute.SelectorConfig) query.OperationSpec {
		return &functions.FirstOpSpec{SelectorConfig: config}
	},
	func(config execute.SelectorConfig) query.OperationSpec {
		return &functions.LastOpSpec{SelectorConfig: config}
	},
	func(config execute.SelectorConfig) query.OperationSpec {
		return &functions.MaxOpSpec{SelectorConfig: config}
	},
	func(config execute.SelectorConfig) query.OperationSpec {
		return &functions.MinOpSpec{SelectorConfig: config}
	},
}

func SelectorTest(fn func(selector query.Operation) (string, *query.Spec)) Fixture {
	_, file, line, _ := runtime.Caller(1)
	fixture := &collection{
		file: filepath.Base(file),
		line: line,
	}

	for _, selectorSpecFn := range selectorCreateFuncs {
		spec := selectorSpecFn(execute.SelectorConfig{
			Column: execute.DefaultValueColLabel,
		})
		op := query.Operation{
			ID:   query.OperationID(fmt.Sprintf("%s0", spec.Kind())),
			Spec: spec,
		}

		fixture.Add(fn(op))
	}
	return fixture
}

func init() {
	RegisterFixture(
		SelectorTest(func(selector query.Operation) (stmt string, spec *query.Spec) {
			return fmt.Sprintf(`SELECT %s(value) FROM db0..cpu`, selector.Spec.Kind()),
				&query.Spec{
					Operations: []*query.Operation{
						{
							ID: "from0",
							Spec: &functions.FromOpSpec{
								Bucket: "db0/autogen",
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
								By: []string{"_measurement"},
							},
						},
						&selector,
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
												Key: &semantic.Identifier{Name: string(selector.Spec.Kind())},
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
						{Parent: "group0", Child: selector.ID},
						{Parent: selector.ID, Child: "map0"},
						{Parent: "map0", Child: "yield0"},
					},
				}
		}),
	)
}
