package spectests

import (
	"time"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/ast"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/functions/inputs"
	"github.com/influxdata/flux/functions/transformations"
	"github.com/influxdata/flux/semantic"
)

func init() {
	RegisterFixture(
		NewFixture(
			`SHOW TAG VALUES ON "db0" FROM "cpu", "mem", "gpu" WITH KEY = "host"`,
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
							Start: flux.Time{
								Relative:   -time.Hour,
								IsRelative: true,
							},
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
										Operator: ast.OrOperator,
										Left: &semantic.BinaryExpression{
											Operator: ast.EqualOperator,
											Left: &semantic.MemberExpression{
												Object:   &semantic.IdentifierExpression{Name: "r"},
												Property: "_measurement",
											},
											Right: &semantic.StringLiteral{Value: "cpu"},
										},
										Right: &semantic.LogicalExpression{
											Operator: ast.OrOperator,
											Left: &semantic.BinaryExpression{
												Operator: ast.EqualOperator,
												Left: &semantic.MemberExpression{
													Object:   &semantic.IdentifierExpression{Name: "r"},
													Property: "_measurement",
												},
												Right: &semantic.StringLiteral{Value: "mem"},
											},
											Right: &semantic.BinaryExpression{
												Operator: ast.EqualOperator,
												Left: &semantic.MemberExpression{
													Object:   &semantic.IdentifierExpression{Name: "r"},
													Property: "_measurement",
												},
												Right: &semantic.StringLiteral{Value: "gpu"},
											},
										},
									},
								},
							},
						},
					},
					{
						ID: "keyValues0",
						Spec: &transformations.KeyValuesOpSpec{
							KeyCols: []string{"host"},
						},
					},
					{
						ID: "group0",
						Spec: &transformations.GroupOpSpec{
							By: []string{"_measurement", "_key"},
						},
					},
					{
						ID: "distinct0",
						Spec: &transformations.DistinctOpSpec{
							Column: execute.DefaultValueColLabel,
						},
					},
					{
						ID: "group1",
						Spec: &transformations.GroupOpSpec{
							By: []string{"_measurement"},
						},
					},
					{
						ID: "rename0",
						Spec: &transformations.RenameOpSpec{
							Cols: map[string]string{
								"_key":   "key",
								"_value": "value",
							},
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
					{Parent: "filter0", Child: "keyValues0"},
					{Parent: "keyValues0", Child: "group0"},
					{Parent: "group0", Child: "distinct0"},
					{Parent: "distinct0", Child: "group1"},
					{Parent: "group1", Child: "rename0"},
					{Parent: "rename0", Child: "yield0"},
				},
				Now: Now(),
			},
		),
	)
}
