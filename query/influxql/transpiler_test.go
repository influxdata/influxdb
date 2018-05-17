package influxql_test

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/ifql/ast"
	"github.com/influxdata/ifql/functions"
	"github.com/influxdata/ifql/query"
	"github.com/influxdata/ifql/query/execute"
	"github.com/influxdata/ifql/semantic"
	influxqllib "github.com/influxdata/influxql"
	"github.com/influxdata/platform/query/influxql"
)

func TestTranspiler(t *testing.T) {
	for _, tt := range []struct {
		s    string
		spec *query.Spec
	}{
		{
			s: `SELECT mean(value) FROM db0..cpu`,
			spec: &query.Spec{
				Operations: []*query.Operation{
					{
						ID: "from0",
						Spec: &functions.FromOpSpec{
							Database: "db0",
						},
					},
					{
						ID: "range0",
						Spec: &functions.RangeOpSpec{
							Start: query.Time{Absolute: time.Unix(0, influxqllib.MinTime)},
							Stop:  query.Time{Absolute: time.Unix(0, influxqllib.MaxTime)},
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
					{
						ID: "mean0",
						Spec: &functions.MeanOpSpec{
							AggregateConfig: execute.AggregateConfig{
								UseStartTime: true,
							},
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
									Properties: []*semantic.Property{{
										Key: &semantic.Identifier{Name: "mean"},
										Value: &semantic.MemberExpression{
											Object: &semantic.IdentifierExpression{
												Name: "r",
											},
											Property: "_value",
										},
									}},
								},
							},
						},
					},
				},
				Edges: []query.Edge{
					{Parent: "from0", Child: "range0"},
					{Parent: "range0", Child: "filter0"},
					{Parent: "filter0", Child: "group0"},
					{Parent: "group0", Child: "mean0"},
					{Parent: "mean0", Child: "map0"},
				},
			},
		},
		{
			s: `SELECT value FROM db0..cpu`,
			spec: &query.Spec{
				Operations: []*query.Operation{
					{
						ID: "from0",
						Spec: &functions.FromOpSpec{
							Database: "db0",
						},
					},
					{
						ID: "range0",
						Spec: &functions.RangeOpSpec{
							Start: query.Time{Absolute: time.Unix(0, influxqllib.MinTime)},
							Stop:  query.Time{Absolute: time.Unix(0, influxqllib.MaxTime)},
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
						ID: "map0",
						Spec: &functions.MapOpSpec{
							Fn: &semantic.FunctionExpression{
								Params: []*semantic.FunctionParam{{
									Key: &semantic.Identifier{Name: "r"},
								}},
								Body: &semantic.ObjectExpression{
									Properties: []*semantic.Property{{
										Key: &semantic.Identifier{Name: "value"},
										Value: &semantic.MemberExpression{
											Object: &semantic.IdentifierExpression{
												Name: "r",
											},
											Property: "_value",
										},
									}},
								},
							},
						},
					},
				},
				Edges: []query.Edge{
					{Parent: "from0", Child: "range0"},
					{Parent: "range0", Child: "filter0"},
					{Parent: "filter0", Child: "map0"},
				},
			},
		},
		{
			s: `SELECT mean(value), max(value) FROM db0..cpu`,
			spec: &query.Spec{
				Operations: []*query.Operation{
					{
						ID: "from0",
						Spec: &functions.FromOpSpec{
							Database: "db0",
						},
					},
					{
						ID: "range0",
						Spec: &functions.RangeOpSpec{
							Start: query.Time{Absolute: time.Unix(0, influxqllib.MinTime)},
							Stop:  query.Time{Absolute: time.Unix(0, influxqllib.MaxTime)},
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
					{
						ID: "mean0",
						Spec: &functions.MeanOpSpec{
							AggregateConfig: execute.AggregateConfig{
								UseStartTime: true,
							},
						},
					},
					{
						ID: "from1",
						Spec: &functions.FromOpSpec{
							Database: "db0",
						},
					},
					{
						ID: "range1",
						Spec: &functions.RangeOpSpec{
							Start: query.Time{Absolute: time.Unix(0, influxqllib.MinTime)},
							Stop:  query.Time{Absolute: time.Unix(0, influxqllib.MaxTime)},
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
											Value: "value",
										},
									},
								},
							},
						},
					},
					{
						ID: "group1",
						Spec: &functions.GroupOpSpec{
							By: []string{"_measurement"},
						},
					},
					{
						ID: "max0",
						Spec: &functions.MaxOpSpec{
							SelectorConfig: execute.SelectorConfig{
								UseStartTime: true,
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
												Property: "val0",
											},
										},
										{
											Key: &semantic.Identifier{Name: "val1"},
											Value: &semantic.MemberExpression{
												Object: &semantic.IdentifierExpression{
													Name: "tables",
												},
												Property: "val1",
											},
										},
									},
								},
							},
							TableNames: map[query.OperationID]string{
								"mean0": "val0",
								"max0":  "val1",
							},
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
											Key: &semantic.Identifier{Name: "mean"},
											Value: &semantic.MemberExpression{
												Object: &semantic.IdentifierExpression{
													Name: "r",
												},
												Property: "val0",
											},
										},
										{
											Key: &semantic.Identifier{Name: "max"},
											Value: &semantic.MemberExpression{
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
					},
				},
				Edges: []query.Edge{
					{Parent: "from0", Child: "range0"},
					{Parent: "range0", Child: "filter0"},
					{Parent: "filter0", Child: "group0"},
					{Parent: "group0", Child: "mean0"},
					{Parent: "from1", Child: "range1"},
					{Parent: "range1", Child: "filter1"},
					{Parent: "filter1", Child: "group1"},
					{Parent: "group1", Child: "max0"},
					{Parent: "mean0", Child: "join0"},
					{Parent: "max0", Child: "join0"},
					{Parent: "join0", Child: "map0"},
				},
			},
		},
		{
			s: `SELECT a + b FROM db0..cpu`,
			spec: &query.Spec{
				Operations: []*query.Operation{
					{
						ID: "from0",
						Spec: &functions.FromOpSpec{
							Database: "db0",
						},
					},
					{
						ID: "range0",
						Spec: &functions.RangeOpSpec{
							Start: query.Time{Absolute: time.Unix(0, influxqllib.MinTime)},
							Stop:  query.Time{Absolute: time.Unix(0, influxqllib.MaxTime)},
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
							Database: "db0",
						},
					},
					{
						ID: "range1",
						Spec: &functions.RangeOpSpec{
							Start: query.Time{Absolute: time.Unix(0, influxqllib.MinTime)},
							Stop:  query.Time{Absolute: time.Unix(0, influxqllib.MaxTime)},
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
												Property: "val0",
											},
										},
										{
											Key: &semantic.Identifier{Name: "val1"},
											Value: &semantic.MemberExpression{
												Object: &semantic.IdentifierExpression{
													Name: "tables",
												},
												Property: "val1",
											},
										},
									},
								},
							},
							TableNames: map[query.OperationID]string{
								"filter0": "val0",
								"filter1": "val1",
							},
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
									Properties: []*semantic.Property{{
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
									}},
								},
							},
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
					{Parent: "join0", Child: "map0"},
				},
			},
		},
	} {
		t.Run(tt.s, func(t *testing.T) {
			if err := tt.spec.Validate(); err != nil {
				t.Fatalf("expected spec is not valid: %s", err)
			}

			transpiler := influxql.NewTranspiler()
			spec, err := transpiler.Transpile(context.Background(), tt.s)
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			} else if err := spec.Validate(); err != nil {
				t.Fatalf("spec is not valid: %s", err)
			}

			// Encode both of these to JSON and compare the results.
			exp, _ := json.Marshal(tt.spec)
			got, _ := json.Marshal(spec)
			if !bytes.Equal(exp, got) {
				// Unmarshal into objects so we can compare the key/value pairs.
				var expObj, gotObj interface{}
				json.Unmarshal(exp, &expObj)
				json.Unmarshal(got, &gotObj)

				// If there is no diff, then they were trivial byte differences and
				// there is no error.
				if diff := cmp.Diff(expObj, gotObj); diff != "" {
					t.Fatalf("unexpected spec:%s", diff)
				}
			}
		})
	}
}
