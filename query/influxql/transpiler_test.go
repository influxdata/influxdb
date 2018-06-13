package influxql_test

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	influxqllib "github.com/influxdata/influxql"
	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/ast"
	"github.com/influxdata/platform/query/execute"
	"github.com/influxdata/platform/query/functions"
	"github.com/influxdata/platform/query/influxql"
	"github.com/influxdata/platform/query/semantic"
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
							Bucket: "db0/autogen",
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
								TimeSrc: execute.DefaultStartColLabel,
								TimeDst: execute.DefaultTimeColLabel,
								Columns: []string{execute.DefaultValueColLabel},
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
											Key: &semantic.Identifier{Name: "_time"},
											Value: &semantic.MemberExpression{
												Object: &semantic.IdentifierExpression{
													Name: "r",
												},
												Property: "_time",
											},
										},
										{
											Key: &semantic.Identifier{Name: "mean"},
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
					{Parent: "group0", Child: "mean0"},
					{Parent: "mean0", Child: "map0"},
					{Parent: "map0", Child: "yield0"},
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
							Bucket: "db0/autogen",
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
					{Parent: "group0", Child: "map0"},
					{Parent: "map0", Child: "yield0"},
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
							Bucket: "db0/autogen",
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
								TimeSrc: execute.DefaultStartColLabel,
								TimeDst: execute.DefaultTimeColLabel,
								Columns: []string{execute.DefaultValueColLabel},
							},
						},
					},
					{
						ID: "from1",
						Spec: &functions.FromOpSpec{
							Bucket: "db0/autogen",
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
								Column: execute.DefaultValueColLabel,
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
								"mean0": "t0",
								"max0":  "t1",
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
											Key: &semantic.Identifier{Name: "_time"},
											Value: &semantic.MemberExpression{
												Object: &semantic.IdentifierExpression{
													Name: "r",
												},
												Property: "_time",
											},
										},
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
					{Parent: "group0", Child: "mean0"},
					{Parent: "from1", Child: "range1"},
					{Parent: "range1", Child: "filter1"},
					{Parent: "filter1", Child: "group1"},
					{Parent: "group1", Child: "max0"},
					{Parent: "mean0", Child: "join0"},
					{Parent: "max0", Child: "join0"},
					{Parent: "join0", Child: "map0"},
					{Parent: "map0", Child: "yield0"},
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
							Bucket: "db0/autogen",
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
							Bucket: "db0/autogen",
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
			},
		},
		{
			s: `SELECT mean(value) FROM db0..cpu WHERE host = 'server01'`,
			spec: &query.Spec{
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
						ID: "filter1",
						Spec: &functions.FilterOpSpec{
							Fn: &semantic.FunctionExpression{
								Params: []*semantic.FunctionParam{
									{Key: &semantic.Identifier{Name: "r"}},
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
								TimeSrc: execute.DefaultStartColLabel,
								TimeDst: execute.DefaultTimeColLabel,
								Columns: []string{execute.DefaultValueColLabel},
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
											Key: &semantic.Identifier{Name: "_time"},
											Value: &semantic.MemberExpression{
												Object: &semantic.IdentifierExpression{
													Name: "r",
												},
												Property: "_time",
											},
										},
										{
											Key: &semantic.Identifier{Name: "mean"},
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
					{Parent: "filter0", Child: "filter1"},
					{Parent: "filter1", Child: "group0"},
					{Parent: "group0", Child: "mean0"},
					{Parent: "mean0", Child: "map0"},
					{Parent: "map0", Child: "yield0"},
				},
			},
		},
		{
			s: `SELECT mean(value) FROM db0..cpu; SELECT max(value) FROM db0..cpu`,
			spec: &query.Spec{
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
								TimeSrc: execute.DefaultStartColLabel,
								TimeDst: execute.DefaultTimeColLabel,
								Columns: []string{execute.DefaultValueColLabel},
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
											Key: &semantic.Identifier{Name: "_time"},
											Value: &semantic.MemberExpression{
												Object: &semantic.IdentifierExpression{
													Name: "r",
												},
												Property: "_time",
											},
										},
										{
											Key: &semantic.Identifier{Name: "mean"},
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
					{
						ID: "from1",
						Spec: &functions.FromOpSpec{
							Bucket: "db0/autogen",
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
								Column: execute.DefaultValueColLabel,
							},
						},
					},
					{
						ID: "map1",
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
											Key: &semantic.Identifier{Name: "max"},
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
						ID: "yield1",
						Spec: &functions.YieldOpSpec{
							Name: "1",
						},
					},
				},
				Edges: []query.Edge{
					{Parent: "from0", Child: "range0"},
					{Parent: "range0", Child: "filter0"},
					{Parent: "filter0", Child: "group0"},
					{Parent: "group0", Child: "mean0"},
					{Parent: "mean0", Child: "map0"},
					{Parent: "map0", Child: "yield0"},
					{Parent: "from1", Child: "range1"},
					{Parent: "range1", Child: "filter1"},
					{Parent: "filter1", Child: "group1"},
					{Parent: "group1", Child: "max0"},
					{Parent: "max0", Child: "map1"},
					{Parent: "map1", Child: "yield1"},
				},
			},
		},
		{
			s: `SELECT value FROM db0.alternate.cpu`,
			spec: &query.Spec{
				Operations: []*query.Operation{
					{
						ID: "from0",
						Spec: &functions.FromOpSpec{
							Bucket: "db0/alternate",
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
					{Parent: "group0", Child: "map0"},
					{Parent: "map0", Child: "yield0"},
				},
			},
		},
		{
			s: `SELECT mean(value) FROM db0..cpu GROUP BY host`,
			spec: &query.Spec{
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
							By: []string{"_measurement", "host"},
						},
					},
					{
						ID: "mean0",
						Spec: &functions.MeanOpSpec{
							AggregateConfig: execute.AggregateConfig{
								TimeSrc: execute.DefaultStartColLabel,
								TimeDst: execute.DefaultTimeColLabel,
								Columns: []string{execute.DefaultValueColLabel},
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
											Key: &semantic.Identifier{Name: "_time"},
											Value: &semantic.MemberExpression{
												Object: &semantic.IdentifierExpression{
													Name: "r",
												},
												Property: "_time",
											},
										},
										{
											Key: &semantic.Identifier{Name: "mean"},
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
					{Parent: "group0", Child: "mean0"},
					{Parent: "mean0", Child: "map0"},
					{Parent: "map0", Child: "yield0"},
				},
			},
		},
		{
			s: `SELECT mean(value) FROM db0..cpu WHERE time >= now() - 10m GROUP BY time(1m)`,
			spec: &query.Spec{
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
							Start: query.Time{Absolute: mustParseTime("2010-09-15T08:50:00Z")},
							Stop:  query.Time{Absolute: mustParseTime("2010-09-15T09:00:00Z")},
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
					{
						ID: "mean0",
						Spec: &functions.MeanOpSpec{
							AggregateConfig: execute.AggregateConfig{
								TimeSrc: execute.DefaultStartColLabel,
								TimeDst: execute.DefaultTimeColLabel,
								Columns: []string{execute.DefaultValueColLabel},
							},
						},
					},
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
											Key: &semantic.Identifier{Name: "mean"},
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
					{Parent: "window0", Child: "mean0"},
					{Parent: "mean0", Child: "window1"},
					{Parent: "window1", Child: "map0"},
					{Parent: "map0", Child: "yield0"},
				},
			},
		},
	} {
		t.Run(tt.s, func(t *testing.T) {
			if err := tt.spec.Validate(); err != nil {
				t.Fatalf("expected spec is not valid: %s", err)
			}

			transpiler := influxql.NewTranspilerWithConfig(influxql.Config{
				NowFn: func() time.Time {
					return mustParseTime("2010-09-15T09:00:00Z")
				},
			})
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

// TestTranspiler_Compile contains the compilation tests from influxdb. It only verifies if
// each of these queries either succeeds or it fails with the proper message for compatibility.
func TestTranspiler_Compile(t *testing.T) {
	for _, tt := range []struct {
		s    string
		err  string // if empty, no error is expected
		skip bool
	}{
		{s: `SELECT time, value FROM cpu`},
		{s: `SELECT value FROM cpu`},
		{s: `SELECT value, host FROM cpu`},
		{s: `SELECT * FROM cpu`, skip: true},
		{s: `SELECT time, * FROM cpu`, skip: true},
		{s: `SELECT value, * FROM cpu`, skip: true},
		{s: `SELECT max(value) FROM cpu`},
		{s: `SELECT max(value), host FROM cpu`},
		{s: `SELECT max(value), * FROM cpu`, skip: true},
		{s: `SELECT max(*) FROM cpu`, skip: true},
		{s: `SELECT max(/val/) FROM cpu`, skip: true},
		{s: `SELECT min(value) FROM cpu`, skip: true},
		{s: `SELECT min(value), host FROM cpu`, skip: true},
		{s: `SELECT min(value), * FROM cpu`, skip: true},
		{s: `SELECT min(*) FROM cpu`, skip: true},
		{s: `SELECT min(/val/) FROM cpu`, skip: true},
		{s: `SELECT first(value) FROM cpu`, skip: true},
		{s: `SELECT first(value), host FROM cpu`, skip: true},
		{s: `SELECT first(value), * FROM cpu`, skip: true},
		{s: `SELECT first(*) FROM cpu`, skip: true},
		{s: `SELECT first(/val/) FROM cpu`, skip: true},
		{s: `SELECT last(value) FROM cpu`, skip: true},
		{s: `SELECT last(value), host FROM cpu`, skip: true},
		{s: `SELECT last(value), * FROM cpu`, skip: true},
		{s: `SELECT last(*) FROM cpu`, skip: true},
		{s: `SELECT last(/val/) FROM cpu`, skip: true},
		{s: `SELECT count(value) FROM cpu`, skip: true},
		{s: `SELECT count(distinct(value)) FROM cpu`, skip: true},
		{s: `SELECT count(distinct value) FROM cpu`, skip: true},
		{s: `SELECT count(*) FROM cpu`, skip: true},
		{s: `SELECT count(/val/) FROM cpu`, skip: true},
		{s: `SELECT mean(value) FROM cpu`},
		{s: `SELECT mean(*) FROM cpu`, skip: true},
		{s: `SELECT mean(/val/) FROM cpu`, skip: true},
		{s: `SELECT min(value), max(value) FROM cpu`, skip: true},
		{s: `SELECT min(*), max(*) FROM cpu`, skip: true},
		{s: `SELECT min(/val/), max(/val/) FROM cpu`, skip: true},
		{s: `SELECT first(value), last(value) FROM cpu`, skip: true},
		{s: `SELECT first(*), last(*) FROM cpu`, skip: true},
		{s: `SELECT first(/val/), last(/val/) FROM cpu`, skip: true},
		{s: `SELECT count(value) FROM cpu WHERE time >= now() - 1h GROUP BY time(10m)`, skip: true},
		{s: `SELECT distinct value FROM cpu`, skip: true},
		{s: `SELECT distinct(value) FROM cpu`, skip: true},
		{s: `SELECT value / total FROM cpu`, skip: true},
		{s: `SELECT min(value) / total FROM cpu`, skip: true},
		{s: `SELECT max(value) / total FROM cpu`, skip: true},
		{s: `SELECT top(value, 1) FROM cpu`, skip: true},
		{s: `SELECT top(value, host, 1) FROM cpu`, skip: true},
		{s: `SELECT top(value, 1), host FROM cpu`, skip: true},
		{s: `SELECT min(top) FROM (SELECT top(value, host, 1) FROM cpu) GROUP BY region`, skip: true},
		{s: `SELECT bottom(value, 1) FROM cpu`, skip: true},
		{s: `SELECT bottom(value, host, 1) FROM cpu`, skip: true},
		{s: `SELECT bottom(value, 1), host FROM cpu`, skip: true},
		{s: `SELECT max(bottom) FROM (SELECT bottom(value, host, 1) FROM cpu) GROUP BY region`, skip: true},
		{s: `SELECT percentile(value, 75) FROM cpu`, skip: true},
		{s: `SELECT percentile(value, 75.0) FROM cpu`, skip: true},
		{s: `SELECT sample(value, 2) FROM cpu`, skip: true},
		{s: `SELECT sample(*, 2) FROM cpu`, skip: true},
		{s: `SELECT sample(/val/, 2) FROM cpu`, skip: true},
		{s: `SELECT elapsed(value) FROM cpu`, skip: true},
		{s: `SELECT elapsed(value, 10s) FROM cpu`, skip: true},
		{s: `SELECT integral(value) FROM cpu`, skip: true},
		{s: `SELECT integral(value, 10s) FROM cpu`, skip: true},
		{s: `SELECT max(value) FROM cpu WHERE time >= now() - 1m GROUP BY time(10s, 5s)`, skip: true},
		{s: `SELECT max(value) FROM cpu WHERE time >= now() - 1m GROUP BY time(10s, '2000-01-01T00:00:05Z')`, skip: true},
		{s: `SELECT max(value) FROM cpu WHERE time >= now() - 1m GROUP BY time(10s, now())`, skip: true},
		{s: `SELECT max(mean) FROM (SELECT mean(value) FROM cpu GROUP BY host)`, skip: true},
		{s: `SELECT max(derivative) FROM (SELECT derivative(mean(value)) FROM cpu) WHERE time >= now() - 1m GROUP BY time(10s)`, skip: true},
		{s: `SELECT max(value) FROM (SELECT value + total FROM cpu) WHERE time >= now() - 1m GROUP BY time(10s)`, skip: true},
		{s: `SELECT value FROM cpu WHERE time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T01:00:00Z'`},
		{s: `SELECT value FROM (SELECT value FROM cpu) ORDER BY time DESC`, skip: true},
		{s: `SELECT count(distinct(value)), max(value) FROM cpu`, skip: true},
		{s: `SELECT derivative(distinct(value)), difference(distinct(value)) FROM cpu WHERE time >= now() - 1m GROUP BY time(5s)`, skip: true},
		{s: `SELECT moving_average(distinct(value), 3) FROM cpu WHERE time >= now() - 5m GROUP BY time(1m)`, skip: true},
		{s: `SELECT elapsed(distinct(value)) FROM cpu WHERE time >= now() - 5m GROUP BY time(1m)`, skip: true},
		{s: `SELECT cumulative_sum(distinct(value)) FROM cpu WHERE time >= now() - 5m GROUP BY time(1m)`, skip: true},
		{s: `SELECT last(value) / (1 - 0) FROM cpu`, skip: true},
		{s: `SELECT abs(value) FROM cpu`, skip: true},
		{s: `SELECT sin(value) FROM cpu`, skip: true},
		{s: `SELECT cos(value) FROM cpu`, skip: true},
		{s: `SELECT tan(value) FROM cpu`, skip: true},
		{s: `SELECT asin(value) FROM cpu`, skip: true},
		{s: `SELECT acos(value) FROM cpu`, skip: true},
		{s: `SELECT atan(value) FROM cpu`, skip: true},
		{s: `SELECT sqrt(value) FROM cpu`, skip: true},
		{s: `SELECT pow(value, 2) FROM cpu`, skip: true},
		{s: `SELECT pow(value, 3.14) FROM cpu`, skip: true},
		{s: `SELECT pow(2, value) FROM cpu`, skip: true},
		{s: `SELECT pow(3.14, value) FROM cpu`, skip: true},
		{s: `SELECT exp(value) FROM cpu`, skip: true},
		{s: `SELECT atan2(value, 0.1) FROM cpu`, skip: true},
		{s: `SELECT atan2(0.2, value) FROM cpu`, skip: true},
		{s: `SELECT atan2(value, 1) FROM cpu`, skip: true},
		{s: `SELECT atan2(2, value) FROM cpu`, skip: true},
		{s: `SELECT ln(value) FROM cpu`, skip: true},
		{s: `SELECT log(value, 2) FROM cpu`, skip: true},
		{s: `SELECT log2(value) FROM cpu`, skip: true},
		{s: `SELECT log10(value) FROM cpu`, skip: true},
		{s: `SELECT sin(value) - sin(1.3) FROM cpu`, skip: true},
		{s: `SELECT value FROM cpu WHERE sin(value) > 0.5`, skip: true},
		{s: `SELECT time FROM cpu`, err: `at least 1 non-time field must be queried`, skip: true},
		{s: `SELECT value, mean(value) FROM cpu`, err: `mixing aggregate and non-aggregate queries is not supported`, skip: true},
		{s: `SELECT value, max(value), min(value) FROM cpu`, err: `mixing multiple selector functions with tags or fields is not supported`, skip: true},
		{s: `SELECT top(value, 10), max(value) FROM cpu`, err: `selector function top() cannot be combined with other functions`, skip: true},
		{s: `SELECT bottom(value, 10), max(value) FROM cpu`, err: `selector function bottom() cannot be combined with other functions`, skip: true},
		{s: `SELECT count() FROM cpu`, err: `invalid number of arguments for count, expected 1, got 0`, skip: true},
		{s: `SELECT count(value, host) FROM cpu`, err: `invalid number of arguments for count, expected 1, got 2`, skip: true},
		{s: `SELECT min() FROM cpu`, err: `invalid number of arguments for min, expected 1, got 0`, skip: true},
		{s: `SELECT min(value, host) FROM cpu`, err: `invalid number of arguments for min, expected 1, got 2`, skip: true},
		{s: `SELECT max() FROM cpu`, err: `invalid number of arguments for max, expected 1, got 0`, skip: true},
		{s: `SELECT max(value, host) FROM cpu`, err: `invalid number of arguments for max, expected 1, got 2`, skip: true},
		{s: `SELECT sum() FROM cpu`, err: `invalid number of arguments for sum, expected 1, got 0`, skip: true},
		{s: `SELECT sum(value, host) FROM cpu`, err: `invalid number of arguments for sum, expected 1, got 2`, skip: true},
		{s: `SELECT first() FROM cpu`, err: `invalid number of arguments for first, expected 1, got 0`, skip: true},
		{s: `SELECT first(value, host) FROM cpu`, err: `invalid number of arguments for first, expected 1, got 2`, skip: true},
		{s: `SELECT last() FROM cpu`, err: `invalid number of arguments for last, expected 1, got 0`, skip: true},
		{s: `SELECT last(value, host) FROM cpu`, err: `invalid number of arguments for last, expected 1, got 2`, skip: true},
		{s: `SELECT mean() FROM cpu`, err: `invalid number of arguments for mean, expected 1, got 0`, skip: true},
		{s: `SELECT mean(value, host) FROM cpu`, err: `invalid number of arguments for mean, expected 1, got 2`, skip: true},
		{s: `SELECT distinct(value), max(value) FROM cpu`, err: `aggregate function distinct() cannot be combined with other functions or fields`, skip: true},
		{s: `SELECT count(distinct()) FROM cpu`, err: `distinct function requires at least one argument`, skip: true},
		{s: `SELECT count(distinct(value, host)) FROM cpu`, err: `distinct function can only have one argument`, skip: true},
		{s: `SELECT count(distinct(2)) FROM cpu`, err: `expected field argument in distinct()`, skip: true},
		{s: `SELECT value FROM cpu GROUP BY now()`, err: `only time() calls allowed in dimensions`},
		{s: `SELECT value FROM cpu GROUP BY time()`, err: `time dimension expected 1 or 2 arguments`},
		{s: `SELECT value FROM cpu GROUP BY time(5m, 30s, 1ms)`, err: `time dimension expected 1 or 2 arguments`},
		{s: `SELECT value FROM cpu GROUP BY time('unexpected')`, err: `time dimension must have duration argument`},
		{s: `SELECT value FROM cpu GROUP BY time(5m), time(1m)`, err: `multiple time dimensions not allowed`},
		{s: `SELECT value FROM cpu GROUP BY time(5m, unexpected())`, err: `time dimension offset function must be now()`, skip: true},
		{s: `SELECT value FROM cpu GROUP BY time(5m, now(1m))`, err: `time dimension offset now() function requires no arguments`, skip: true},
		{s: `SELECT value FROM cpu GROUP BY time(5m, 'unexpected')`, err: `time dimension offset must be duration or now()`, skip: true},
		{s: `SELECT value FROM cpu GROUP BY 'unexpected'`, err: `only time and tag dimensions allowed`},
		{s: `SELECT top(value) FROM cpu`, err: `invalid number of arguments for top, expected at least 2, got 1`, skip: true},
		{s: `SELECT top('unexpected', 5) FROM cpu`, err: `expected first argument to be a field in top(), found 'unexpected'`, skip: true},
		{s: `SELECT top(value, 'unexpected', 5) FROM cpu`, err: `only fields or tags are allowed in top(), found 'unexpected'`, skip: true},
		{s: `SELECT top(value, 2.5) FROM cpu`, err: `expected integer as last argument in top(), found 2.500`, skip: true},
		{s: `SELECT top(value, -1) FROM cpu`, err: `limit (-1) in top function must be at least 1`, skip: true},
		{s: `SELECT top(value, 3) FROM cpu LIMIT 2`, err: `limit (3) in top function can not be larger than the LIMIT (2) in the select statement`, skip: true},
		{s: `SELECT bottom(value) FROM cpu`, err: `invalid number of arguments for bottom, expected at least 2, got 1`, skip: true},
		{s: `SELECT bottom('unexpected', 5) FROM cpu`, err: `expected first argument to be a field in bottom(), found 'unexpected'`, skip: true},
		{s: `SELECT bottom(value, 'unexpected', 5) FROM cpu`, err: `only fields or tags are allowed in bottom(), found 'unexpected'`, skip: true},
		{s: `SELECT bottom(value, 2.5) FROM cpu`, err: `expected integer as last argument in bottom(), found 2.500`, skip: true},
		{s: `SELECT bottom(value, -1) FROM cpu`, err: `limit (-1) in bottom function must be at least 1`, skip: true},
		{s: `SELECT bottom(value, 3) FROM cpu LIMIT 2`, err: `limit (3) in bottom function can not be larger than the LIMIT (2) in the select statement`, skip: true},
		// TODO(jsternberg): This query is wrong, but we cannot enforce this because of previous behavior: https://github.com/influxdata/influxdb/pull/8771
		//{s: `SELECT value FROM cpu WHERE time >= now() - 10m OR time < now() - 5m`, err: `cannot use OR with time conditions`},
		{s: `SELECT value FROM cpu WHERE value`, err: `invalid condition expression: value`},
		{s: `SELECT count(value), * FROM cpu`, err: `mixing aggregate and non-aggregate queries is not supported`, skip: true},
		{s: `SELECT max(*), host FROM cpu`, err: `mixing aggregate and non-aggregate queries is not supported`, skip: true},
		{s: `SELECT count(value), /ho/ FROM cpu`, err: `mixing aggregate and non-aggregate queries is not supported`, skip: true},
		{s: `SELECT max(/val/), * FROM cpu`, err: `mixing aggregate and non-aggregate queries is not supported`, skip: true},
		{s: `SELECT a(value) FROM cpu`, err: `undefined function a()`, skip: true},
		{s: `SELECT count(max(value)) FROM myseries`, err: `expected field argument in count()`, skip: true},
		{s: `SELECT count(distinct('value')) FROM myseries`, err: `expected field argument in distinct()`, skip: true},
		{s: `SELECT distinct('value') FROM myseries`, err: `expected field argument in distinct()`, skip: true},
		{s: `SELECT min(max(value)) FROM myseries`, err: `expected field argument in min()`, skip: true},
		{s: `SELECT min(distinct(value)) FROM myseries`, err: `expected field argument in min()`, skip: true},
		{s: `SELECT max(max(value)) FROM myseries`, err: `expected field argument in max()`, skip: true},
		{s: `SELECT sum(max(value)) FROM myseries`, err: `expected field argument in sum()`, skip: true},
		{s: `SELECT first(max(value)) FROM myseries`, err: `expected field argument in first()`, skip: true},
		{s: `SELECT last(max(value)) FROM myseries`, err: `expected field argument in last()`, skip: true},
		{s: `SELECT mean(max(value)) FROM myseries`, err: `expected field argument in mean()`, skip: true},
		{s: `SELECT median(max(value)) FROM myseries`, err: `expected field argument in median()`, skip: true},
		{s: `SELECT mode(max(value)) FROM myseries`, err: `expected field argument in mode()`, skip: true},
		{s: `SELECT stddev(max(value)) FROM myseries`, err: `expected field argument in stddev()`, skip: true},
		{s: `SELECT spread(max(value)) FROM myseries`, err: `expected field argument in spread()`, skip: true},
		{s: `SELECT top() FROM myseries`, err: `invalid number of arguments for top, expected at least 2, got 0`, skip: true},
		{s: `SELECT top(field1) FROM myseries`, err: `invalid number of arguments for top, expected at least 2, got 1`, skip: true},
		{s: `SELECT top(field1,foo) FROM myseries`, err: `expected integer as last argument in top(), found foo`, skip: true},
		{s: `SELECT top(field1,host,'server',foo) FROM myseries`, err: `expected integer as last argument in top(), found foo`, skip: true},
		{s: `SELECT top(field1,5,'server',2) FROM myseries`, err: `only fields or tags are allowed in top(), found 5`, skip: true},
		{s: `SELECT top(field1,max(foo),'server',2) FROM myseries`, err: `only fields or tags are allowed in top(), found max(foo)`, skip: true},
		{s: `SELECT top(value, 10) + count(value) FROM myseries`, err: `selector function top() cannot be combined with other functions`, skip: true},
		{s: `SELECT top(max(value), 10) FROM myseries`, err: `expected first argument to be a field in top(), found max(value)`, skip: true},
		{s: `SELECT bottom() FROM myseries`, err: `invalid number of arguments for bottom, expected at least 2, got 0`, skip: true},
		{s: `SELECT bottom(field1) FROM myseries`, err: `invalid number of arguments for bottom, expected at least 2, got 1`, skip: true},
		{s: `SELECT bottom(field1,foo) FROM myseries`, err: `expected integer as last argument in bottom(), found foo`, skip: true},
		{s: `SELECT bottom(field1,host,'server',foo) FROM myseries`, err: `expected integer as last argument in bottom(), found foo`, skip: true},
		{s: `SELECT bottom(field1,5,'server',2) FROM myseries`, err: `only fields or tags are allowed in bottom(), found 5`, skip: true},
		{s: `SELECT bottom(field1,max(foo),'server',2) FROM myseries`, err: `only fields or tags are allowed in bottom(), found max(foo)`, skip: true},
		{s: `SELECT bottom(value, 10) + count(value) FROM myseries`, err: `selector function bottom() cannot be combined with other functions`, skip: true},
		{s: `SELECT bottom(max(value), 10) FROM myseries`, err: `expected first argument to be a field in bottom(), found max(value)`, skip: true},
		{s: `SELECT top(value, 10), bottom(value, 10) FROM cpu`, err: `selector function top() cannot be combined with other functions`, skip: true},
		{s: `SELECT bottom(value, 10), top(value, 10) FROM cpu`, err: `selector function bottom() cannot be combined with other functions`, skip: true},
		{s: `SELECT sample(value) FROM myseries`, err: `invalid number of arguments for sample, expected 2, got 1`, skip: true},
		{s: `SELECT sample(value, 2, 3) FROM myseries`, err: `invalid number of arguments for sample, expected 2, got 3`, skip: true},
		{s: `SELECT sample(value, 0) FROM myseries`, err: `sample window must be greater than 1, got 0`, skip: true},
		{s: `SELECT sample(value, 2.5) FROM myseries`, err: `expected integer argument in sample()`, skip: true},
		{s: `SELECT percentile() FROM myseries`, err: `invalid number of arguments for percentile, expected 2, got 0`, skip: true},
		{s: `SELECT percentile(field1) FROM myseries`, err: `invalid number of arguments for percentile, expected 2, got 1`, skip: true},
		{s: `SELECT percentile(field1, foo) FROM myseries`, err: `expected float argument in percentile()`, skip: true},
		{s: `SELECT percentile(max(field1), 75) FROM myseries`, err: `expected field argument in percentile()`, skip: true},
		{s: `SELECT field1 FROM foo group by time(1s)`, err: `GROUP BY requires at least one aggregate function`, skip: true},
		{s: `SELECT field1 FROM foo fill(none)`, err: `fill(none) must be used with a function`, skip: true},
		{s: `SELECT field1 FROM foo fill(linear)`, err: `fill(linear) must be used with a function`, skip: true},
		{s: `SELECT count(value), value FROM foo`, err: `mixing aggregate and non-aggregate queries is not supported`, skip: true},
		{s: `SELECT count(value) FROM foo group by time`, err: `time() is a function and expects at least one argument`},
		{s: `SELECT count(value) FROM foo group by 'time'`, err: `only time and tag dimensions allowed`},
		{s: `SELECT count(value) FROM foo where time > now() and time < now() group by time()`, err: `time dimension expected 1 or 2 arguments`},
		{s: `SELECT count(value) FROM foo where time > now() and time < now() group by time(b)`, err: `time dimension must have duration argument`},
		{s: `SELECT count(value) FROM foo where time > now() and time < now() group by time(1s), time(2s)`, err: `multiple time dimensions not allowed`},
		{s: `SELECT count(value) FROM foo where time > now() and time < now() group by time(1s, b)`, err: `time dimension offset must be duration or now()`, skip: true},
		{s: `SELECT count(value) FROM foo where time > now() and time < now() group by time(1s, '5s')`, err: `time dimension offset must be duration or now()`, skip: true},
		{s: `SELECT distinct(field1), sum(field1) FROM myseries`, err: `aggregate function distinct() cannot be combined with other functions or fields`, skip: true},
		{s: `SELECT distinct(field1), field2 FROM myseries`, err: `aggregate function distinct() cannot be combined with other functions or fields`, skip: true},
		{s: `SELECT distinct(field1, field2) FROM myseries`, err: `distinct function can only have one argument`, skip: true},
		{s: `SELECT distinct() FROM myseries`, err: `distinct function requires at least one argument`, skip: true},
		{s: `SELECT distinct field1, field2 FROM myseries`, err: `aggregate function distinct() cannot be combined with other functions or fields`, skip: true},
		{s: `SELECT count(distinct field1, field2) FROM myseries`, err: `invalid number of arguments for count, expected 1, got 2`, skip: true},
		{s: `select count(distinct(too, many, arguments)) from myseries`, err: `distinct function can only have one argument`, skip: true},
		{s: `select count() from myseries`, err: `invalid number of arguments for count, expected 1, got 0`, skip: true},
		{s: `SELECT derivative(field1), field1 FROM myseries`, err: `mixing aggregate and non-aggregate queries is not supported`, skip: true},
		{s: `select derivative() from myseries`, err: `invalid number of arguments for derivative, expected at least 1 but no more than 2, got 0`, skip: true},
		{s: `select derivative(mean(value), 1h, 3) from myseries`, err: `invalid number of arguments for derivative, expected at least 1 but no more than 2, got 3`, skip: true},
		{s: `SELECT derivative(value) FROM myseries group by time(1h)`, err: `aggregate function required inside the call to derivative`, skip: true},
		{s: `SELECT derivative(top(value)) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for top, expected at least 2, got 1`, skip: true},
		{s: `SELECT derivative(bottom(value)) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for bottom, expected at least 2, got 1`, skip: true},
		{s: `SELECT derivative(max()) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for max, expected 1, got 0`, skip: true},
		{s: `SELECT derivative(percentile(value)) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for percentile, expected 2, got 1`, skip: true},
		{s: `SELECT derivative(mean(value), 1h) FROM myseries where time < now() and time > now() - 1d`, err: `derivative aggregate requires a GROUP BY interval`, skip: true},
		{s: `SELECT derivative(value, -2h) FROM myseries`, err: `duration argument must be positive, got -2h`, skip: true},
		{s: `SELECT derivative(value, 10) FROM myseries`, err: `second argument to derivative must be a duration, got *influxql.IntegerLiteral`, skip: true},
		{s: `SELECT derivative(f, true) FROM myseries`, err: `second argument to derivative must be a duration, got *influxql.BooleanLiteral`, skip: true},
		{s: `SELECT non_negative_derivative(field1), field1 FROM myseries`, err: `mixing aggregate and non-aggregate queries is not supported`, skip: true},
		{s: `select non_negative_derivative() from myseries`, err: `invalid number of arguments for non_negative_derivative, expected at least 1 but no more than 2, got 0`, skip: true},
		{s: `select non_negative_derivative(mean(value), 1h, 3) from myseries`, err: `invalid number of arguments for non_negative_derivative, expected at least 1 but no more than 2, got 3`, skip: true},
		{s: `SELECT non_negative_derivative(value) FROM myseries group by time(1h)`, err: `aggregate function required inside the call to non_negative_derivative`, skip: true},
		{s: `SELECT non_negative_derivative(top(value)) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for top, expected at least 2, got 1`, skip: true},
		{s: `SELECT non_negative_derivative(bottom(value)) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for bottom, expected at least 2, got 1`, skip: true},
		{s: `SELECT non_negative_derivative(max()) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for max, expected 1, got 0`, skip: true},
		{s: `SELECT non_negative_derivative(mean(value), 1h) FROM myseries where time < now() and time > now() - 1d`, err: `non_negative_derivative aggregate requires a GROUP BY interval`, skip: true},
		{s: `SELECT non_negative_derivative(percentile(value)) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for percentile, expected 2, got 1`, skip: true},
		{s: `SELECT non_negative_derivative(value, -2h) FROM myseries`, err: `duration argument must be positive, got -2h`, skip: true},
		{s: `SELECT non_negative_derivative(value, 10) FROM myseries`, err: `second argument to non_negative_derivative must be a duration, got *influxql.IntegerLiteral`, skip: true},
		{s: `SELECT difference(field1), field1 FROM myseries`, err: `mixing aggregate and non-aggregate queries is not supported`, skip: true},
		{s: `SELECT difference() from myseries`, err: `invalid number of arguments for difference, expected 1, got 0`, skip: true},
		{s: `SELECT difference(value) FROM myseries group by time(1h)`, err: `aggregate function required inside the call to difference`, skip: true},
		{s: `SELECT difference(top(value)) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for top, expected at least 2, got 1`, skip: true},
		{s: `SELECT difference(bottom(value)) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for bottom, expected at least 2, got 1`, skip: true},
		{s: `SELECT difference(max()) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for max, expected 1, got 0`, skip: true},
		{s: `SELECT difference(percentile(value)) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for percentile, expected 2, got 1`, skip: true},
		{s: `SELECT difference(mean(value)) FROM myseries where time < now() and time > now() - 1d`, err: `difference aggregate requires a GROUP BY interval`, skip: true},
		{s: `SELECT non_negative_difference(field1), field1 FROM myseries`, err: `mixing aggregate and non-aggregate queries is not supported`, skip: true},
		{s: `SELECT non_negative_difference() from myseries`, err: `invalid number of arguments for non_negative_difference, expected 1, got 0`, skip: true},
		{s: `SELECT non_negative_difference(value) FROM myseries group by time(1h)`, err: `aggregate function required inside the call to non_negative_difference`, skip: true},
		{s: `SELECT non_negative_difference(top(value)) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for top, expected at least 2, got 1`, skip: true},
		{s: `SELECT non_negative_difference(bottom(value)) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for bottom, expected at least 2, got 1`, skip: true},
		{s: `SELECT non_negative_difference(max()) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for max, expected 1, got 0`, skip: true},
		{s: `SELECT non_negative_difference(percentile(value)) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for percentile, expected 2, got 1`, skip: true},
		{s: `SELECT non_negative_difference(mean(value)) FROM myseries where time < now() and time > now() - 1d`, err: `non_negative_difference aggregate requires a GROUP BY interval`, skip: true},
		{s: `SELECT elapsed() FROM myseries`, err: `invalid number of arguments for elapsed, expected at least 1 but no more than 2, got 0`, skip: true},
		{s: `SELECT elapsed(value) FROM myseries group by time(1h)`, err: `aggregate function required inside the call to elapsed`, skip: true},
		{s: `SELECT elapsed(value, 1s, host) FROM myseries`, err: `invalid number of arguments for elapsed, expected at least 1 but no more than 2, got 3`, skip: true},
		{s: `SELECT elapsed(value, 0s) FROM myseries`, err: `duration argument must be positive, got 0s`, skip: true},
		{s: `SELECT elapsed(value, -10s) FROM myseries`, err: `duration argument must be positive, got -10s`, skip: true},
		{s: `SELECT elapsed(value, 10) FROM myseries`, err: `second argument to elapsed must be a duration, got *influxql.IntegerLiteral`, skip: true},
		{s: `SELECT elapsed(top(value)) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for top, expected at least 2, got 1`, skip: true},
		{s: `SELECT elapsed(bottom(value)) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for bottom, expected at least 2, got 1`, skip: true},
		{s: `SELECT elapsed(max()) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for max, expected 1, got 0`, skip: true},
		{s: `SELECT elapsed(percentile(value)) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for percentile, expected 2, got 1`, skip: true},
		{s: `SELECT elapsed(mean(value)) FROM myseries where time < now() and time > now() - 1d`, err: `elapsed aggregate requires a GROUP BY interval`, skip: true},
		{s: `SELECT moving_average(field1, 2), field1 FROM myseries`, err: `mixing aggregate and non-aggregate queries is not supported`, skip: true},
		{s: `SELECT moving_average(field1, 1), field1 FROM myseries`, err: `moving_average window must be greater than 1, got 1`, skip: true},
		{s: `SELECT moving_average(field1, 0), field1 FROM myseries`, err: `moving_average window must be greater than 1, got 0`, skip: true},
		{s: `SELECT moving_average(field1, -1), field1 FROM myseries`, err: `moving_average window must be greater than 1, got -1`, skip: true},
		{s: `SELECT moving_average(field1, 2.0), field1 FROM myseries`, err: `second argument for moving_average must be an integer, got *influxql.NumberLiteral`, skip: true},
		{s: `SELECT moving_average() from myseries`, err: `invalid number of arguments for moving_average, expected 2, got 0`, skip: true},
		{s: `SELECT moving_average(value) FROM myseries`, err: `invalid number of arguments for moving_average, expected 2, got 1`, skip: true},
		{s: `SELECT moving_average(value, 2) FROM myseries group by time(1h)`, err: `aggregate function required inside the call to moving_average`, skip: true},
		{s: `SELECT moving_average(top(value), 2) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for top, expected at least 2, got 1`, skip: true},
		{s: `SELECT moving_average(bottom(value), 2) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for bottom, expected at least 2, got 1`, skip: true},
		{s: `SELECT moving_average(max(), 2) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for max, expected 1, got 0`, skip: true},
		{s: `SELECT moving_average(percentile(value), 2) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for percentile, expected 2, got 1`, skip: true},
		{s: `SELECT moving_average(mean(value), 2) FROM myseries where time < now() and time > now() - 1d`, err: `moving_average aggregate requires a GROUP BY interval`, skip: true},
		{s: `SELECT cumulative_sum(field1), field1 FROM myseries`, err: `mixing aggregate and non-aggregate queries is not supported`, skip: true},
		{s: `SELECT cumulative_sum() from myseries`, err: `invalid number of arguments for cumulative_sum, expected 1, got 0`, skip: true},
		{s: `SELECT cumulative_sum(value) FROM myseries group by time(1h)`, err: `aggregate function required inside the call to cumulative_sum`, skip: true},
		{s: `SELECT cumulative_sum(top(value)) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for top, expected at least 2, got 1`, skip: true},
		{s: `SELECT cumulative_sum(bottom(value)) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for bottom, expected at least 2, got 1`, skip: true},
		{s: `SELECT cumulative_sum(max()) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for max, expected 1, got 0`, skip: true},
		{s: `SELECT cumulative_sum(percentile(value)) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for percentile, expected 2, got 1`, skip: true},
		{s: `SELECT cumulative_sum(mean(value)) FROM myseries where time < now() and time > now() - 1d`, err: `cumulative_sum aggregate requires a GROUP BY interval`, skip: true},
		{s: `SELECT integral() FROM myseries`, err: `invalid number of arguments for integral, expected at least 1 but no more than 2, got 0`, skip: true},
		{s: `SELECT integral(value, 10s, host) FROM myseries`, err: `invalid number of arguments for integral, expected at least 1 but no more than 2, got 3`, skip: true},
		{s: `SELECT integral(value, -10s) FROM myseries`, err: `duration argument must be positive, got -10s`, skip: true},
		{s: `SELECT integral(value, 10) FROM myseries`, err: `second argument must be a duration`, skip: true},
		{s: `SELECT holt_winters(value) FROM myseries where time < now() and time > now() - 1d`, err: `invalid number of arguments for holt_winters, expected 3, got 1`, skip: true},
		{s: `SELECT holt_winters(value, 10, 2) FROM myseries where time < now() and time > now() - 1d`, err: `must use aggregate function with holt_winters`, skip: true},
		{s: `SELECT holt_winters(min(value), 10, 2) FROM myseries where time < now() and time > now() - 1d`, err: `holt_winters aggregate requires a GROUP BY interval`, skip: true},
		{s: `SELECT holt_winters(min(value), 0, 2) FROM myseries where time < now() and time > now() - 1d GROUP BY time(1d)`, err: `second arg to holt_winters must be greater than 0, got 0`, skip: true},
		{s: `SELECT holt_winters(min(value), false, 2) FROM myseries where time < now() and time > now() - 1d GROUP BY time(1d)`, err: `expected integer argument as second arg in holt_winters`, skip: true},
		{s: `SELECT holt_winters(min(value), 10, 'string') FROM myseries where time < now() and time > now() - 1d GROUP BY time(1d)`, err: `expected integer argument as third arg in holt_winters`, skip: true},
		{s: `SELECT holt_winters(min(value), 10, -1) FROM myseries where time < now() and time > now() - 1d GROUP BY time(1d)`, err: `third arg to holt_winters cannot be negative, got -1`, skip: true},
		{s: `SELECT holt_winters_with_fit(value) FROM myseries where time < now() and time > now() - 1d`, err: `invalid number of arguments for holt_winters_with_fit, expected 3, got 1`, skip: true},
		{s: `SELECT holt_winters_with_fit(value, 10, 2) FROM myseries where time < now() and time > now() - 1d`, err: `must use aggregate function with holt_winters_with_fit`, skip: true},
		{s: `SELECT holt_winters_with_fit(min(value), 10, 2) FROM myseries where time < now() and time > now() - 1d`, err: `holt_winters_with_fit aggregate requires a GROUP BY interval`, skip: true},
		{s: `SELECT holt_winters_with_fit(min(value), 0, 2) FROM myseries where time < now() and time > now() - 1d GROUP BY time(1d)`, err: `second arg to holt_winters_with_fit must be greater than 0, got 0`, skip: true},
		{s: `SELECT holt_winters_with_fit(min(value), false, 2) FROM myseries where time < now() and time > now() - 1d GROUP BY time(1d)`, err: `expected integer argument as second arg in holt_winters_with_fit`, skip: true},
		{s: `SELECT holt_winters_with_fit(min(value), 10, 'string') FROM myseries where time < now() and time > now() - 1d GROUP BY time(1d)`, err: `expected integer argument as third arg in holt_winters_with_fit`, skip: true},
		{s: `SELECT holt_winters_with_fit(min(value), 10, -1) FROM myseries where time < now() and time > now() - 1d GROUP BY time(1d)`, err: `third arg to holt_winters_with_fit cannot be negative, got -1`, skip: true},
		{s: `SELECT mean(value) + value FROM cpu WHERE time < now() and time > now() - 1h GROUP BY time(10m)`, err: `mixing aggregate and non-aggregate queries is not supported`, skip: true},
		// TODO: Remove this restriction in the future: https://github.com/influxdata/influxdb/issues/5968
		{s: `SELECT mean(cpu_total - cpu_idle) FROM cpu`, err: `expected field argument in mean()`, skip: true},
		{s: `SELECT derivative(mean(cpu_total - cpu_idle), 1s) FROM cpu WHERE time < now() AND time > now() - 1d GROUP BY time(1h)`, err: `expected field argument in mean()`, skip: true},
		// TODO: The error message will change when math is allowed inside an aggregate: https://github.com/influxdata/influxdb/pull/5990#issuecomment-195565870
		{s: `SELECT count(foo + sum(bar)) FROM cpu`, err: `expected field argument in count()`, skip: true},
		{s: `SELECT (count(foo + sum(bar))) FROM cpu`, err: `expected field argument in count()`, skip: true},
		{s: `SELECT sum(value) + count(foo + sum(bar)) FROM cpu`, err: `expected field argument in count()`, skip: true},
		{s: `SELECT top(value, 2), max(value) FROM cpu`, err: `selector function top() cannot be combined with other functions`, skip: true},
		{s: `SELECT bottom(value, 2), max(value) FROM cpu`, err: `selector function bottom() cannot be combined with other functions`, skip: true},
		{s: `SELECT min(derivative) FROM (SELECT derivative(mean(value), 1h) FROM myseries) where time < now() and time > now() - 1d`, err: `derivative aggregate requires a GROUP BY interval`, skip: true},
		{s: `SELECT min(mean) FROM (SELECT mean(value) FROM myseries GROUP BY time)`, err: `time() is a function and expects at least one argument`, skip: true},
		{s: `SELECT value FROM myseries WHERE value OR time >= now() - 1m`, err: `invalid condition expression: value`},
		{s: `SELECT value FROM myseries WHERE time >= now() - 1m OR value`, err: `invalid condition expression: value`},
		{s: `SELECT value FROM (SELECT value FROM cpu ORDER BY time DESC) ORDER BY time ASC`, err: `subqueries must be ordered in the same direction as the query itself`, skip: true},
		{s: `SELECT sin(value, 3) FROM cpu`, err: `invalid number of arguments for sin, expected 1, got 2`, skip: true},
		{s: `SELECT cos(2.3, value, 3) FROM cpu`, err: `invalid number of arguments for cos, expected 1, got 3`, skip: true},
		{s: `SELECT tan(value, 3) FROM cpu`, err: `invalid number of arguments for tan, expected 1, got 2`, skip: true},
		{s: `SELECT asin(value, 3) FROM cpu`, err: `invalid number of arguments for asin, expected 1, got 2`, skip: true},
		{s: `SELECT acos(value, 3.2) FROM cpu`, err: `invalid number of arguments for acos, expected 1, got 2`, skip: true},
		{s: `SELECT atan() FROM cpu`, err: `invalid number of arguments for atan, expected 1, got 0`, skip: true},
		{s: `SELECT sqrt(42, 3, 4) FROM cpu`, err: `invalid number of arguments for sqrt, expected 1, got 3`, skip: true},
		{s: `SELECT abs(value, 3) FROM cpu`, err: `invalid number of arguments for abs, expected 1, got 2`, skip: true},
		{s: `SELECT ln(value, 3) FROM cpu`, err: `invalid number of arguments for ln, expected 1, got 2`, skip: true},
		{s: `SELECT log2(value, 3) FROM cpu`, err: `invalid number of arguments for log2, expected 1, got 2`, skip: true},
		{s: `SELECT log10(value, 3) FROM cpu`, err: `invalid number of arguments for log10, expected 1, got 2`, skip: true},
		{s: `SELECT pow(value, 3, 3) FROM cpu`, err: `invalid number of arguments for pow, expected 2, got 3`, skip: true},
		{s: `SELECT atan2(value, 3, 3) FROM cpu`, err: `invalid number of arguments for atan2, expected 2, got 3`, skip: true},
		{s: `SELECT sin(1.3) FROM cpu`, err: `field must contain at least one variable`, skip: true},
		{s: `SELECT nofunc(1.3) FROM cpu`, err: `undefined function nofunc()`, skip: true},
	} {
		t.Run(tt.s, func(t *testing.T) {
			defer func() {
				if err := recover(); err != nil {
					if tt.skip {
						t.Skip()
					}
					t.Errorf("panic: %s", err)
				}
			}()

			transpiler := influxql.NewTranspilerWithConfig(influxql.Config{
				DefaultDatabase: "db0",
			})
			if _, err := transpiler.Transpile(context.Background(), tt.s); err != nil {
				if got, want := err.Error(), tt.err; got != want {
					if tt.skip {
						t.Skip()
					}
					t.Errorf("unexpected error: got=%q want=%q", got, want)
				} else if tt.skip {
					t.Errorf("marked as a skipped test, but it succeeds")
				}
			} else {
				if tt.err != "" {
					if tt.skip {
						t.Skip()
					}
					t.Errorf("expected error")
				} else if tt.skip {
					t.Errorf("marked as a skipped test, but it succeeds")
				}
			}
		})
	}
}
