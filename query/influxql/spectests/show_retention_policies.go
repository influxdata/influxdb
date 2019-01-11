package spectests

import (
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/ast"
	"github.com/influxdata/flux/functions/transformations"
	"github.com/influxdata/flux/semantic"
	"github.com/influxdata/influxdb/query/functions/inputs"
)

func init() {
	RegisterFixture(
		NewFixture(
			`SHOW RETENTION POLICIES ON telegraf`,
			&flux.Spec{
				Operations: []*flux.Operation{
					{
						ID:   "databases0",
						Spec: &inputs.DatabasesOpSpec{},
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
									Body: &semantic.BinaryExpression{
										Operator: ast.EqualOperator,
										Left: &semantic.MemberExpression{
											Object:   &semantic.IdentifierExpression{Name: "r"},
											Property: "databaseName",
										},
										Right: &semantic.StringLiteral{Value: "telegraf"},
									},
								},
							},
						},
					},
					{
						ID: "rename0",
						Spec: &transformations.RenameOpSpec{
							Columns: map[string]string{
								"retentionPolicy": "name",
								"retentionPeriod": "duration",
							},
						},
					},
					{
						ID: "set0",
						Spec: &transformations.SetOpSpec{
							Key:   "shardGroupDuration",
							Value: "0",
						},
					},
					{
						ID: "set1",
						Spec: &transformations.SetOpSpec{
							Key:   "replicaN",
							Value: "2",
						},
					},
					{
						ID: "keep0",
						Spec: &transformations.KeepOpSpec{
							Columns: []string{
								"name",
								"duration",
								"shardGroupDuration",
								"replicaN",
								"default",
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
					{Parent: "databases0", Child: "filter0"},
					{Parent: "filter0", Child: "rename0"},
					{Parent: "rename0", Child: "set0"},
					{Parent: "set0", Child: "set1"},
					{Parent: "set1", Child: "keep0"},
					{Parent: "keep0", Child: "yield0"},
				},
				Now: Now(),
			},
		),
	)
}
