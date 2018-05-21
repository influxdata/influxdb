package promql

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/influxdata/ifql/ast"
	"github.com/influxdata/ifql/functions"
	"github.com/influxdata/ifql/query"
	"github.com/influxdata/ifql/semantic"
	"github.com/influxdata/ifql/semantic/semantictest"
)

func TestParsePromQL(t *testing.T) {
	tests := []struct {
		name    string
		promql  string
		opts    []Option
		want    interface{}
		wantErr bool
	}{
		{
			name:   "testing comments",
			promql: "# http_requests_total",
			want: &Comment{
				Source: "# http_requests_total",
			},
		},
		{
			name:   "vector",
			promql: "http_requests_total",
			want: &Selector{
				Name: "http_requests_total",
			},
		},
		{
			name:   "vector with label matching",
			promql: `http_requests_total{a="b"}`,
			want: &Selector{
				Name: "http_requests_total",
				LabelMatchers: []*LabelMatcher{
					&LabelMatcher{
						Name: "a",
						Kind: Equal,
						Value: &StringLiteral{
							String: "b",
						},
					},
				},
			},
		},
		{
			name:   "vector with two labels matching",
			promql: `http_requests_total{a="b", c!="d"}`,
			want: &Selector{
				Name: "http_requests_total",
				LabelMatchers: []*LabelMatcher{
					&LabelMatcher{
						Name: "a",
						Kind: Equal,
						Value: &StringLiteral{
							String: "b",
						},
					},
					&LabelMatcher{
						Name: "c",
						Kind: NotEqual,
						Value: &StringLiteral{
							String: "d",
						},
					},
				},
			},
		},
		{
			name:   "vector with numeric label matcher",
			promql: `http_requests_total{a=500}`,
			want: &Selector{
				Name: "http_requests_total",
				LabelMatchers: []*LabelMatcher{
					&LabelMatcher{
						Name: "a",
						Kind: Equal,
						Value: &Number{
							Val: 500,
						},
					},
				},
			},
		},
		{
			name:    "invalid operator in label matcher",
			promql:  `http_requests_total{a > 500}`,
			wantErr: true,
			want:    "",
		},
		{
			name:    "no metric name",
			promql:  `{}`,
			wantErr: true,
			want:    "",
		},
		{
			name:   "vector with multiple regular expressions",
			promql: `foo{a="b", foo!="bar", test=~"test", bar!~"baz"}`,
			want: &Selector{
				Name: "foo",
				LabelMatchers: []*LabelMatcher{
					&LabelMatcher{
						Name: "a",
						Kind: Equal,
						Value: &StringLiteral{
							String: "b",
						},
					},
					&LabelMatcher{
						Name: "foo",
						Kind: NotEqual,
						Value: &StringLiteral{
							String: "bar",
						},
					},
					&LabelMatcher{
						Name: "test",
						Kind: RegexMatch,
						Value: &StringLiteral{
							String: "test",
						},
					},
					&LabelMatcher{
						Name: "bar",
						Kind: RegexNoMatch,
						Value: &StringLiteral{
							String: "baz",
						},
					},
				},
			},
		},
		{
			name:   "vector with offset",
			promql: "http_requests_total OFFSET 5m",
			want: &Selector{
				Name:   "http_requests_total",
				Offset: time.Minute * 5,
			},
		},
		{
			name:   "vector with range",
			promql: "http_requests_total[5y]",
			want: &Selector{
				Name:  "http_requests_total",
				Range: time.Hour * 24 * 365 * 5,
			},
		},
		{
			name:   "vector with label matches, range, and offset",
			promql: `test{a="b"}[5y] OFFSET 3d`,
			want: &Selector{
				Name:   "test",
				Offset: time.Hour * 24 * 3,
				Range:  time.Hour * 24 * 365 * 5,
				LabelMatchers: []*LabelMatcher{
					&LabelMatcher{
						Name: "a",
						Kind: Equal,
						Value: &StringLiteral{
							String: "b",
						},
					},
				},
			},
		},
		{
			name:   "Min function with group by and keep common",
			promql: `MIN (some_metric) by (foo) keep_common`,
			want: &AggregateExpr{
				Op: &Operator{
					Kind: MinKind,
				},
				Selector: &Selector{
					Name: "some_metric",
				},
				Aggregate: &Aggregate{
					By: true,
					Labels: []*Identifier{
						&Identifier{
							Name: "foo",
						},
					},
				},
			},
		},
		{
			name:   "count function with group by and keep common reversed with label",
			promql: `COUNT by (foo) keep_common (some_metric)`,
			want: &AggregateExpr{
				Op: &Operator{
					Kind: CountKind,
				},
				Selector: &Selector{
					Name: "some_metric",
				},
				Aggregate: &Aggregate{
					By: true,
					Labels: []*Identifier{
						&Identifier{
							Name: "foo",
						},
					},
				},
			},
		},
		{
			name:   "avg function with group by and no keep common reversed with label",
			promql: `avg by (foo)(some_metric)`,
			want: &AggregateExpr{
				Op: &Operator{
					Kind: AvgKind,
				},
				Selector: &Selector{
					Name: "some_metric",
				},
				Aggregate: &Aggregate{
					By: true,
					Labels: []*Identifier{
						&Identifier{
							Name: "foo",
						},
					},
				},
			},
		},
		{
			name:   "sum function with multiple group by and keep common",
			promql: `sum (some_metric) by (foo,bar) keep_common`,
			want: &AggregateExpr{
				Op: &Operator{
					Kind: SumKind,
				},
				Selector: &Selector{
					Name: "some_metric",
				},
				Aggregate: &Aggregate{
					By: true,
					Labels: []*Identifier{
						&Identifier{
							Name: "foo",
						},
						&Identifier{
							Name: "bar",
						},
					},
				},
			},
		},
		{
			name:   "sum function with group by",
			promql: `sum by (foo)(some_metric)`,
			want: &AggregateExpr{
				Op: &Operator{
					Kind: SumKind,
				},
				Selector: &Selector{
					Name: "some_metric",
				},
				Aggregate: &Aggregate{
					By: true,
					Labels: []*Identifier{
						&Identifier{
							Name: "foo",
						},
					},
				},
			},
		},
		{
			name:   "sum function without reversed label",
			promql: `sum without (foo) (some_metric)`,
			want: &AggregateExpr{
				Op: &Operator{
					Kind: SumKind,
				},
				Selector: &Selector{
					Name: "some_metric",
				},
				Aggregate: &Aggregate{
					Without: true,
					Labels: []*Identifier{
						&Identifier{
							Name: "foo",
						},
					},
				},
			},
		},
		{
			name:   "count_values with argument",
			promql: `count_values("version", build_version)`,
			want: &AggregateExpr{
				Op: &Operator{
					Kind: CountValuesKind,
					Arg: &StringLiteral{
						String: "version",
					},
				},
				Selector: &Selector{
					Name: "build_version",
				},
			},
		},
		{
			name:   "sum over a range",
			promql: `sum(node_cpu{_measurement="m0"}[170h])`,
			want: &AggregateExpr{
				Op: &Operator{
					Kind: SumKind,
				},
				Selector: &Selector{
					Name:  "node_cpu",
					Range: 170 * time.Hour,
					LabelMatchers: []*LabelMatcher{
						{
							Name: "_measurement",
							Kind: Equal,
							Value: &StringLiteral{
								String: "m0",
							},
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParsePromQL(tt.promql, tt.opts...)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParsePromQL() %s error = %v, wantErr %v", tt.promql, err, tt.wantErr)
				return
			}
			if !cmp.Equal(tt.want, got) {
				t.Errorf("ParsePromQL() = %s -got/+want %s", tt.promql, cmp.Diff(tt.want, got))
			}
		})
	}
}

func TestBuild(t *testing.T) {
	tests := []struct {
		name    string
		promql  string
		opts    []Option
		want    *query.Spec
		wantErr bool
	}{
		{
			name:   "aggregate with count without a group by",
			promql: `count(node_cpu{mode="user",cpu="cpu2"})`,
			want: &query.Spec{
				Operations: []*query.Operation{
					{
						ID:   query.OperationID("from"),
						Spec: &functions.FromOpSpec{Database: "prometheus"},
					},
					{
						ID: "where",
						Spec: &functions.FilterOpSpec{
							Fn: &semantic.FunctionExpression{
								Params: []*semantic.FunctionParam{{Key: &semantic.Identifier{Name: "r"}}},
								Body: &semantic.LogicalExpression{
									Operator: ast.AndOperator,
									Left: &semantic.LogicalExpression{
										Operator: ast.AndOperator,
										Left: &semantic.BinaryExpression{
											Operator: ast.EqualOperator,
											Left: &semantic.MemberExpression{
												Object: &semantic.IdentifierExpression{
													Name: "r",
												},
												Property: "_metric",
											},
											Right: &semantic.StringLiteral{
												Value: "node_cpu",
											},
										},
										Right: &semantic.BinaryExpression{
											Operator: ast.EqualOperator,
											Left: &semantic.MemberExpression{
												Object: &semantic.IdentifierExpression{
													Name: "r",
												},
												Property: "mode",
											},
											Right: &semantic.StringLiteral{
												Value: "user",
											},
										},
									},
									Right: &semantic.BinaryExpression{
										Operator: ast.EqualOperator,
										Left: &semantic.MemberExpression{
											Object: &semantic.IdentifierExpression{
												Name: "r",
											},
											Property: "cpu",
										},
										Right: &semantic.StringLiteral{
											Value: "cpu2",
										},
									},
								},
							},
						},
					},
					{
						ID: query.OperationID("count"), Spec: &functions.CountOpSpec{},
					},
				},
				Edges: []query.Edge{
					{
						Parent: query.OperationID("from"),
						Child:  query.OperationID("where"),
					},
					{
						Parent: query.OperationID("where"),
						Child:  query.OperationID("count"),
					},
				},
			},
		},
		{
			name:   "range of time but no aggregates",
			promql: `node_cpu{mode="user"}[2m] offset 5m`,
			want: &query.Spec{
				Operations: []*query.Operation{
					{
						ID:   query.OperationID("from"),
						Spec: &functions.FromOpSpec{Database: "prometheus"},
					},
					{
						ID: query.OperationID("range"),
						Spec: &functions.RangeOpSpec{
							Start: query.Time{Relative: -time.Minute * 7},
						},
					},
					{
						ID: "where",
						Spec: &functions.FilterOpSpec{
							Fn: &semantic.FunctionExpression{
								Params: []*semantic.FunctionParam{{Key: &semantic.Identifier{Name: "r"}}},
								Body: &semantic.LogicalExpression{
									Operator: ast.AndOperator,
									Left: &semantic.BinaryExpression{
										Operator: ast.EqualOperator,
										Left: &semantic.MemberExpression{
											Object: &semantic.IdentifierExpression{
												Name: "r",
											},
											Property: "_metric",
										},
										Right: &semantic.StringLiteral{
											Value: "node_cpu",
										},
									},
									Right: &semantic.BinaryExpression{
										Operator: ast.EqualOperator,
										Left: &semantic.MemberExpression{
											Object: &semantic.IdentifierExpression{
												Name: "r",
											},
											Property: "mode",
										},
										Right: &semantic.StringLiteral{
											Value: "user",
										},
									},
								},
							},
						},
					},
				},
				Edges: []query.Edge{
					{
						Parent: query.OperationID("from"),
						Child:  query.OperationID("range"),
					},
					{
						Parent: query.OperationID("range"),
						Child:  query.OperationID("where"),
					},
				},
			},
		},

		{
			name:   "sum over a range",
			promql: `sum(node_cpu{_measurement="m0"}[170h])`,
			want: &query.Spec{
				Operations: []*query.Operation{
					{
						ID:   query.OperationID("from"),
						Spec: &functions.FromOpSpec{Database: "prometheus"},
					},
					{
						ID: query.OperationID("range"),
						Spec: &functions.RangeOpSpec{
							Start: query.Time{Relative: -170 * time.Hour},
						},
					},
					{
						ID: "where",
						Spec: &functions.FilterOpSpec{
							Fn: &semantic.FunctionExpression{
								Params: []*semantic.FunctionParam{{Key: &semantic.Identifier{Name: "r"}}},
								Body: &semantic.LogicalExpression{
									Operator: ast.AndOperator,
									Left: &semantic.BinaryExpression{
										Operator: ast.EqualOperator,
										Left: &semantic.MemberExpression{
											Object: &semantic.IdentifierExpression{
												Name: "r",
											},
											Property: "_metric",
										},
										Right: &semantic.StringLiteral{
											Value: "node_cpu",
										},
									},
									Right: &semantic.BinaryExpression{
										Operator: ast.EqualOperator,
										Left: &semantic.MemberExpression{
											Object: &semantic.IdentifierExpression{
												Name: "r",
											},
											Property: "_measurement",
										},
										Right: &semantic.StringLiteral{
											Value: "m0",
										},
									},
								},
							},
						},
					},
					{
						ID: query.OperationID("sum"), Spec: &functions.SumOpSpec{},
					},
				},
				Edges: []query.Edge{
					{
						Parent: query.OperationID("from"),
						Child:  query.OperationID("range"),
					},
					{
						Parent: query.OperationID("range"),
						Child:  query.OperationID("where"),
					},
					{
						Parent: query.OperationID("where"),
						Child:  query.OperationID("sum"),
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Build(tt.promql, tt.opts...)

			if (err != nil) != tt.wantErr {
				t.Errorf("Build() %s error = %v, wantErr %v", tt.promql, err, tt.wantErr)
				return
			}
			opts := append(semantictest.CmpOptions, []cmp.Option{cmp.AllowUnexported(query.Spec{}), cmpopts.IgnoreUnexported(query.Spec{})}...)
			if !cmp.Equal(tt.want, got, opts...) {
				t.Errorf("Build() = %s -want/+got\n%s", tt.promql, cmp.Diff(tt.want, got, opts...))
			}
		})
	}
}
