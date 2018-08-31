package functions_test

import (
	"regexp"
	"testing"

	"github.com/influxdata/platform/query/ast"
	"github.com/influxdata/platform/query/execute/executetest"
	"github.com/influxdata/platform/query/plan"
	"github.com/influxdata/platform/query/semantic"
	"github.com/influxdata/platform/query/values"
	"github.com/pkg/errors"

	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/execute"
	"github.com/influxdata/platform/query/functions"
	"github.com/influxdata/platform/query/querytest"
)

func TestSchemaMutions_NewQueries(t *testing.T) {
	tests := []querytest.NewQueryTestCase{
		{
			Name: "test rename query",
			Raw:  `from(bucket:"mybucket") |> rename(columns:{old:"new"}) |> sum()`,
			Want: &query.Spec{
				Operations: []*query.Operation{
					{
						ID: "from0",
						Spec: &functions.FromOpSpec{
							Bucket: "mybucket",
						},
					},
					{
						ID: "rename1",
						Spec: &functions.RenameOpSpec{
							Cols: map[string]string{
								"old": "new",
							},
						},
					},
					{
						ID: "sum2",
						Spec: &functions.SumOpSpec{
							AggregateConfig: execute.DefaultAggregateConfig,
						},
					},
				},
				Edges: []query.Edge{
					{Parent: "from0", Child: "rename1"},
					{Parent: "rename1", Child: "sum2"},
				},
			},
		},
		{
			Name: "test drop query",
			Raw:  `from(bucket:"mybucket") |> drop(columns:["col1", "col2", "col3"]) |> sum()`,
			Want: &query.Spec{
				Operations: []*query.Operation{
					{
						ID: "from0",
						Spec: &functions.FromOpSpec{
							Bucket: "mybucket",
						},
					},
					{
						ID: "drop1",
						Spec: &functions.DropOpSpec{
							Cols: []string{"col1", "col2", "col3"},
						},
					},
					{
						ID: "sum2",
						Spec: &functions.SumOpSpec{
							AggregateConfig: execute.DefaultAggregateConfig,
						},
					},
				},
				Edges: []query.Edge{
					{Parent: "from0", Child: "drop1"},
					{Parent: "drop1", Child: "sum2"},
				},
			},
		},
		{
			Name: "test keep query",
			Raw:  `from(bucket:"mybucket") |> keep(columns:["col1", "col2", "col3"]) |> sum()`,
			Want: &query.Spec{
				Operations: []*query.Operation{
					{
						ID: "from0",
						Spec: &functions.FromOpSpec{
							Bucket: "mybucket",
						},
					},
					{
						ID: "keep1",
						Spec: &functions.KeepOpSpec{
							Cols: []string{"col1", "col2", "col3"},
						},
					},
					{
						ID: "sum2",
						Spec: &functions.SumOpSpec{
							AggregateConfig: execute.DefaultAggregateConfig,
						},
					},
				},
				Edges: []query.Edge{
					{Parent: "from0", Child: "keep1"},
					{Parent: "keep1", Child: "sum2"},
				},
			},
		},
		{
			Name: "test duplicate query",
			Raw:  `from(bucket:"mybucket") |> duplicate(column: "col1", as: "col1_new") |> sum()`,
			Want: &query.Spec{
				Operations: []*query.Operation{
					{
						ID: "from0",
						Spec: &functions.FromOpSpec{
							Bucket: "mybucket",
						},
					},
					{
						ID: "duplicate1",
						Spec: &functions.DuplicateOpSpec{
							Col: "col1",
							As:  "col1_new",
						},
					},
					{
						ID: "sum2",
						Spec: &functions.SumOpSpec{
							AggregateConfig: execute.DefaultAggregateConfig,
						},
					},
				},
				Edges: []query.Edge{
					{Parent: "from0", Child: "duplicate1"},
					{Parent: "duplicate1", Child: "sum2"},
				},
			},
		},
		{
			Name: "test drop query fn param",
			Raw:  `from(bucket:"mybucket") |> drop(fn: (col) => col =~ /reg*/) |> sum()`,
			Want: &query.Spec{
				Operations: []*query.Operation{
					{
						ID: "from0",
						Spec: &functions.FromOpSpec{
							Bucket: "mybucket",
						},
					},
					{
						ID: "drop1",
						Spec: &functions.DropOpSpec{
							Predicate: &semantic.FunctionExpression{
								Params: []*semantic.FunctionParam{{Key: &semantic.Identifier{Name: "col"}}},
								Body: &semantic.BinaryExpression{
									Operator: ast.RegexpMatchOperator,
									Left: &semantic.IdentifierExpression{
										Name: "col",
									},
									Right: &semantic.RegexpLiteral{
										Value: regexp.MustCompile(`reg*`),
									},
								},
							},
						},
					},
					{
						ID: "sum2",
						Spec: &functions.SumOpSpec{
							AggregateConfig: execute.DefaultAggregateConfig,
						},
					},
				},
				Edges: []query.Edge{
					{Parent: "from0", Child: "drop1"},
					{Parent: "drop1", Child: "sum2"},
				},
			},
		},
		{
			Name: "test keep query fn param",
			Raw:  `from(bucket:"mybucket") |> keep(fn: (col) => col =~ /reg*/) |> sum()`,
			Want: &query.Spec{
				Operations: []*query.Operation{
					{
						ID: "from0",
						Spec: &functions.FromOpSpec{
							Bucket: "mybucket",
						},
					},
					{
						ID: "keep1",
						Spec: &functions.KeepOpSpec{
							Predicate: &semantic.FunctionExpression{
								Params: []*semantic.FunctionParam{{Key: &semantic.Identifier{Name: "col"}}},
								Body: &semantic.BinaryExpression{
									Operator: ast.RegexpMatchOperator,
									Left: &semantic.IdentifierExpression{
										Name: "col",
									},
									Right: &semantic.RegexpLiteral{
										Value: regexp.MustCompile(`reg*`),
									},
								},
							},
						},
					},
					{
						ID: "sum2",
						Spec: &functions.SumOpSpec{
							AggregateConfig: execute.DefaultAggregateConfig,
						},
					},
				},
				Edges: []query.Edge{
					{Parent: "from0", Child: "keep1"},
					{Parent: "keep1", Child: "sum2"},
				},
			},
		},
		{
			Name: "test rename query fn param",
			Raw:  `from(bucket:"mybucket") |> rename(fn: (col) => "new_name") |> sum()`,
			Want: &query.Spec{
				Operations: []*query.Operation{
					{
						ID: "from0",
						Spec: &functions.FromOpSpec{
							Bucket: "mybucket",
						},
					},
					{
						ID: "rename1",
						Spec: &functions.RenameOpSpec{
							Fn: &semantic.FunctionExpression{
								Params: []*semantic.FunctionParam{{Key: &semantic.Identifier{Name: "col"}}},
								Body: &semantic.StringLiteral{
									Value: "new_name",
								},
							},
						},
					},
					{
						ID: "sum2",
						Spec: &functions.SumOpSpec{
							AggregateConfig: execute.DefaultAggregateConfig,
						},
					},
				},
				Edges: []query.Edge{
					{Parent: "from0", Child: "rename1"},
					{Parent: "rename1", Child: "sum2"},
				},
			},
		},
		{
			Name:    "test rename query invalid",
			Raw:     `from(bucket:"mybucket") |> rename(fn: (col) => "new_name", columns: {a:"b", c:"d"}) |> sum()`,
			Want:    nil,
			WantErr: true,
		},
		{
			Name:    "test drop query invalid",
			Raw:     `from(bucket:"mybucket") |> drop(fn: (col) => col == target, columns: ["a", "b"]) |> sum()`,
			Want:    nil,
			WantErr: true,
		},
		{
			Name:    "test keep query invalid",
			Raw:     `from(bucket:"mybucket") |> keep(fn: (col) => col == target, columns: ["a", "b"]) |> sum()`,
			Want:    nil,
			WantErr: true,
		},
		{
			Name:    "test duplicate query invalid",
			Raw:     `from(bucket:"mybucket") |> duplicate(columns: ["a", "b"], n: -1) |> sum()`,
			Want:    nil,
			WantErr: true,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()
			querytest.NewQueryTestHelper(t, tc)
		})
	}
}

func TestDropRenameKeep_Process(t *testing.T) {
	testCases := []struct {
		name    string
		spec    plan.ProcedureSpec
		data    []query.Table
		want    []*executetest.Table
		wantErr error
	}{
		{
			name: "rename multiple cols",
			spec: &functions.SchemaMutationProcedureSpec{
				Mutations: []functions.SchemaMutation{
					&functions.RenameOpSpec{
						Cols: map[string]string{
							"1a": "1b",
							"2a": "2b",
							"3a": "3b",
						},
					},
				},
			},
			data: []query.Table{&executetest.Table{
				ColMeta: []query.ColMeta{
					{Label: "1a", Type: query.TFloat},
					{Label: "2a", Type: query.TFloat},
					{Label: "3a", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{1.0, 2.0, 3.0},
					{11.0, 12.0, 13.0},
					{21.0, 22.0, 23.0},
				},
			}},
			want: []*executetest.Table{{
				ColMeta: []query.ColMeta{
					{Label: "1b", Type: query.TFloat},
					{Label: "2b", Type: query.TFloat},
					{Label: "3b", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{1.0, 2.0, 3.0},
					{11.0, 12.0, 13.0},
					{21.0, 22.0, 23.0},
				},
			}},
		},

		{
			name: "drop multiple cols",
			spec: &functions.SchemaMutationProcedureSpec{
				Mutations: []functions.SchemaMutation{
					&functions.DropOpSpec{
						Cols: []string{"a", "b"},
					},
				},
			},
			data: []query.Table{&executetest.Table{
				ColMeta: []query.ColMeta{
					{Label: "a", Type: query.TFloat},
					{Label: "b", Type: query.TFloat},
					{Label: "c", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{1.0, 2.0, 3.0},
					{11.0, 12.0, 13.0},
					{21.0, 22.0, 23.0},
				},
			}},
			want: []*executetest.Table{{
				ColMeta: []query.ColMeta{
					{Label: "c", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{3.0},
					{13.0},
					{23.0},
				},
			}},
		},
		{
			name: "keep multiple cols",
			spec: &functions.SchemaMutationProcedureSpec{
				Mutations: []functions.SchemaMutation{
					&functions.KeepOpSpec{
						Cols: []string{"a"},
					},
				},
			},
			data: []query.Table{&executetest.Table{
				ColMeta: []query.ColMeta{
					{Label: "a", Type: query.TFloat},
					{Label: "b", Type: query.TFloat},
					{Label: "c", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{1.0, 2.0, 3.0},
					{11.0, 12.0, 13.0},
					{21.0, 22.0, 23.0},
				},
			}},
			want: []*executetest.Table{{
				ColMeta: []query.ColMeta{
					{Label: "a", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{1.0},
					{11.0},
					{21.0},
				},
			}},
		},
		{
			name: "duplicate single col",
			spec: &functions.SchemaMutationProcedureSpec{
				Mutations: []functions.SchemaMutation{
					&functions.DuplicateOpSpec{
						Col: "a",
						As:  "a_1",
					},
				},
			},
			data: []query.Table{&executetest.Table{
				ColMeta: []query.ColMeta{
					{Label: "a", Type: query.TFloat},
					{Label: "b", Type: query.TFloat},
					{Label: "c", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{1.0, 2.0, 3.0},
					{11.0, 12.0, 13.0},
					{21.0, 22.0, 23.0},
				},
			}},
			want: []*executetest.Table{{
				ColMeta: []query.ColMeta{
					{Label: "a", Type: query.TFloat},
					{Label: "a_1", Type: query.TFloat},
					{Label: "b", Type: query.TFloat},
					{Label: "c", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{1.0, 1.0, 2.0, 3.0},
					{11.0, 11.0, 12.0, 13.0},
					{21.0, 21.0, 22.0, 23.0},
				},
			}},
		},
		{
			name: "rename map fn (col) => name",
			spec: &functions.SchemaMutationProcedureSpec{
				Mutations: []functions.SchemaMutation{
					&functions.RenameOpSpec{
						Fn: &semantic.FunctionExpression{
							Params: []*semantic.FunctionParam{{Key: &semantic.Identifier{Name: "col"}}},
							Body: &semantic.StringLiteral{
								Value: "new_name",
							},
						},
					},
				},
			},
			data: []query.Table{&executetest.Table{
				ColMeta: []query.ColMeta{
					{Label: "1a", Type: query.TFloat},
					{Label: "2a", Type: query.TFloat},
					{Label: "3a", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{1.0, 2.0, 3.0},
					{11.0, 12.0, 13.0},
					{21.0, 22.0, 23.0},
				},
			}},
			want: []*executetest.Table{{
				ColMeta: []query.ColMeta{
					{Label: "new_name", Type: query.TFloat},
					{Label: "new_name", Type: query.TFloat},
					{Label: "new_name", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{1.0, 2.0, 3.0},
					{11.0, 12.0, 13.0},
					{21.0, 22.0, 23.0},
				},
			}},
		},
		{
			name: "drop predicate (col) => col ~= /reg/",
			spec: &functions.SchemaMutationProcedureSpec{
				Mutations: []functions.SchemaMutation{
					&functions.DropOpSpec{
						Predicate: &semantic.FunctionExpression{
							Params: []*semantic.FunctionParam{{Key: &semantic.Identifier{Name: "col"}}},
							Body: &semantic.BinaryExpression{
								Operator: ast.RegexpMatchOperator,
								Left: &semantic.IdentifierExpression{
									Name: "col",
								},
								Right: &semantic.RegexpLiteral{
									Value: regexp.MustCompile(`server*`),
								},
							},
						},
					},
				},
			},
			data: []query.Table{&executetest.Table{
				ColMeta: []query.ColMeta{
					{Label: "server1", Type: query.TFloat},
					{Label: "local", Type: query.TFloat},
					{Label: "server2", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{1.0, 2.0, 3.0},
					{11.0, 12.0, 13.0},
					{21.0, 22.0, 23.0},
				},
			}},
			want: []*executetest.Table{{
				ColMeta: []query.ColMeta{
					{Label: "local", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{2.0},
					{12.0},
					{22.0},
				},
			}},
		},
		{
			name: "keep predicate (col) => col ~= /reg/",
			spec: &functions.SchemaMutationProcedureSpec{
				Mutations: []functions.SchemaMutation{
					&functions.KeepOpSpec{
						Predicate: &semantic.FunctionExpression{
							Params: []*semantic.FunctionParam{{Key: &semantic.Identifier{Name: "col"}}},
							Body: &semantic.BinaryExpression{
								Operator: ast.RegexpMatchOperator,
								Left: &semantic.IdentifierExpression{
									Name: "col",
								},
								Right: &semantic.RegexpLiteral{
									Value: regexp.MustCompile(`server*`),
								},
							},
						},
					},
				},
			},
			data: []query.Table{&executetest.Table{
				ColMeta: []query.ColMeta{
					{Label: "server1", Type: query.TFloat},
					{Label: "local", Type: query.TFloat},
					{Label: "server2", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{1.0, 2.0, 3.0},
					{11.0, 12.0, 13.0},
					{21.0, 22.0, 23.0},
				},
			}},
			want: []*executetest.Table{{
				ColMeta: []query.ColMeta{
					{Label: "server1", Type: query.TFloat},
					{Label: "server2", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{1.0, 3.0},
					{11.0, 13.0},
					{21.0, 23.0},
				},
			}},
		},
		{
			name: "drop and rename",
			spec: &functions.SchemaMutationProcedureSpec{
				Mutations: []functions.SchemaMutation{
					&functions.DropOpSpec{
						Cols: []string{"server1", "server2"},
					},
					&functions.RenameOpSpec{
						Cols: map[string]string{
							"local": "localhost",
						},
					},
				},
			},
			data: []query.Table{&executetest.Table{
				ColMeta: []query.ColMeta{
					{Label: "server1", Type: query.TFloat},
					{Label: "local", Type: query.TFloat},
					{Label: "server2", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{1.0, 2.0, 3.0},
					{11.0, 12.0, 13.0},
					{21.0, 22.0, 23.0},
				},
			}},
			want: []*executetest.Table{{
				ColMeta: []query.ColMeta{
					{Label: "localhost", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{2.0},
					{12.0},
					{22.0},
				},
			}},
		},
		{
			name: "drop no exist",
			spec: &functions.SchemaMutationProcedureSpec{
				Mutations: []functions.SchemaMutation{
					&functions.DropOpSpec{
						Cols: []string{"no_exist"},
					},
				},
			},
			data: []query.Table{&executetest.Table{
				ColMeta: []query.ColMeta{
					{Label: "server1", Type: query.TFloat},
					{Label: "local", Type: query.TFloat},
					{Label: "server2", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{1.0, 2.0, 3.0},
					{11.0, 12.0, 13.0},
					{21.0, 22.0, 23.0},
				},
			}},
			want:    []*executetest.Table(nil),
			wantErr: errors.New(`drop error: column "no_exist" doesn't exist`),
		},
		{
			name: "rename no exist",
			spec: &functions.SchemaMutationProcedureSpec{
				Mutations: []functions.SchemaMutation{
					&functions.RenameOpSpec{
						Cols: map[string]string{
							"no_exist": "noexist",
						},
					},
				},
			},
			data: []query.Table{&executetest.Table{
				ColMeta: []query.ColMeta{
					{Label: "server1", Type: query.TFloat},
					{Label: "local", Type: query.TFloat},
					{Label: "server2", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{1.0, 2.0, 3.0},
					{11.0, 12.0, 13.0},
					{21.0, 22.0, 23.0},
				},
			}},
			want:    []*executetest.Table(nil),
			wantErr: errors.New(`rename error: column "no_exist" doesn't exist`),
		},
		{
			name: "keep no exist",
			spec: &functions.SchemaMutationProcedureSpec{
				Mutations: []functions.SchemaMutation{
					&functions.KeepOpSpec{
						Cols: []string{"no_exist"},
					},
				},
			},
			data: []query.Table{&executetest.Table{
				ColMeta: []query.ColMeta{
					{Label: "server1", Type: query.TFloat},
					{Label: "local", Type: query.TFloat},
					{Label: "server2", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{1.0, 2.0, 3.0},
					{11.0, 12.0, 13.0},
					{21.0, 22.0, 23.0},
				},
			}},
			want:    []*executetest.Table(nil),
			wantErr: errors.New(`keep error: column "no_exist" doesn't exist`),
		},
		{
			name: "duplicate no exist",
			spec: &functions.SchemaMutationProcedureSpec{
				Mutations: []functions.SchemaMutation{
					&functions.DuplicateOpSpec{
						Col: "no_exist",
						As:  "no_exist_2",
					},
				},
			},
			data: []query.Table{&executetest.Table{
				ColMeta: []query.ColMeta{
					{Label: "server1", Type: query.TFloat},
					{Label: "local", Type: query.TFloat},
					{Label: "server2", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{1.0, 2.0, 3.0},
					{11.0, 12.0, 13.0},
					{21.0, 22.0, 23.0},
				},
			}},
			want:    []*executetest.Table(nil),
			wantErr: errors.New(`duplicate error: column "no_exist" doesn't exist`),
		},
		{
			name: "rename group key",
			spec: &functions.SchemaMutationProcedureSpec{
				Mutations: []functions.SchemaMutation{
					&functions.RenameOpSpec{
						Cols: map[string]string{
							"1a": "1b",
							"2a": "2b",
							"3a": "3b",
						},
					},
				},
			},
			data: []query.Table{&executetest.Table{
				ColMeta: []query.ColMeta{
					{Label: "1a", Type: query.TFloat},
					{Label: "2a", Type: query.TFloat},
					{Label: "3a", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{1.0, 2.0, 3.0},
					{1.0, 12.0, 13.0},
					{1.0, 22.0, 23.0},
				},
				KeyCols:   []string{"1a"},
				KeyValues: []interface{}{1.0},
				GroupKey: execute.NewGroupKey(
					[]query.ColMeta{{
						Label: "1a",
						Type:  query.TFloat,
					}},
					[]values.Value{values.NewFloatValue(1.0)},
				),
			}},
			want: []*executetest.Table{{
				ColMeta: []query.ColMeta{
					{Label: "1b", Type: query.TFloat},
					{Label: "2b", Type: query.TFloat},
					{Label: "3b", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{1.0, 2.0, 3.0},
					{1.0, 12.0, 13.0},
					{1.0, 22.0, 23.0},
				},
				KeyCols:   []string{"1b"},
				KeyValues: []interface{}{1.0},
				GroupKey: execute.NewGroupKey(
					[]query.ColMeta{{
						Label: "1b",
						Type:  query.TFloat,
					}},
					[]values.Value{values.NewFloatValue(1.0)},
				),
			}},
		},
		{
			name: "drop group key",
			spec: &functions.SchemaMutationProcedureSpec{
				Mutations: []functions.SchemaMutation{
					&functions.DropOpSpec{
						Cols: []string{"2a"},
					},
				},
			},
			data: []query.Table{&executetest.Table{
				ColMeta: []query.ColMeta{
					{Label: "1a", Type: query.TFloat},
					{Label: "2a", Type: query.TFloat},
					{Label: "3a", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{1.0, 2.0, 3.0},
					{11.0, 2.0, 13.0},
					{21.0, 2.0, 23.0},
				},
				KeyCols:   []string{"2a"},
				KeyValues: []interface{}{2.0},
				GroupKey: execute.NewGroupKey(
					[]query.ColMeta{{
						Label: "2a",
						Type:  query.TFloat,
					}},
					[]values.Value{values.NewFloatValue(2.0)},
				),
			}},
			want: []*executetest.Table{{
				ColMeta: []query.ColMeta{
					{Label: "1a", Type: query.TFloat},
					{Label: "3a", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{1.0, 3.0},
					{11.0, 13.0},
					{21.0, 23.0},
				},
				KeyCols:   []string(nil),
				KeyValues: []interface{}(nil),
				GroupKey:  execute.NewGroupKey([]query.ColMeta{}, []values.Value{}),
			}},
		},
		{
			name: "keep group key",
			spec: &functions.SchemaMutationProcedureSpec{
				Mutations: []functions.SchemaMutation{
					&functions.KeepOpSpec{
						Cols: []string{"1a"},
					},
				},
			},
			data: []query.Table{&executetest.Table{
				ColMeta: []query.ColMeta{
					{Label: "1a", Type: query.TFloat},
					{Label: "2a", Type: query.TFloat},
					{Label: "3a", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{1.0, 2.0, 3.0},
					{1.0, 12.0, 3.0},
					{1.0, 22.0, 3.0},
				},
				KeyCols:   []string{"1a", "3a"},
				KeyValues: []interface{}{1.0, 3.0},
				GroupKey: execute.NewGroupKey(
					[]query.ColMeta{
						{Label: "1a", Type: query.TFloat},
						{Label: "3a", Type: query.TFloat},
					},
					[]values.Value{values.NewFloatValue(1.0), values.NewFloatValue(3.0)},
				),
			}},
			want: []*executetest.Table{{
				ColMeta: []query.ColMeta{
					{Label: "1a", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{1.0},
					{1.0},
					{1.0},
				},
				KeyCols:   []string{"1a"},
				KeyValues: []interface{}{1.0},
				GroupKey: execute.NewGroupKey(
					[]query.ColMeta{
						{Label: "1a", Type: query.TFloat},
					},
					[]values.Value{values.NewFloatValue(1.0)},
				),
			}},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			executetest.ProcessTestHelper(
				t,
				tc.data,
				tc.want,
				tc.wantErr,
				func(d execute.Dataset, c execute.TableBuilderCache) execute.Transformation {
					tr, err := functions.NewSchemaMutationTransformation(d, c, tc.spec)
					if err != nil {
						t.Fatal(err)
					}
					return tr
				},
			)
		})
	}
}

// TODO: determine SchemaMutationProcedureSpec pushdown/rewrite rules
/*
func TestRenameDrop_PushDown(t *testing.T) {
	m1, _ := functions.NewRenameMutator(&functions.RenameOpSpec{
		Cols: map[string]string{},
	})

	root := &plan.Procedure{
		Spec: &functions.SchemaMutationProcedureSpec{
			Mutations: []functions.SchemaMutator{m1},
		},
	}

	m2, _ := functions.NewDropKeepMutator(&functions.DropOpSpec{
		Cols: []string{},
	})

	m3, _ := functions.NewDropKeepMutator(&functions.KeepOpSpec{
		Cols: []string{},
	})

	spec := &functions.SchemaMutationProcedureSpec{
		Mutations: []functions.SchemaMutator{m2, m3},
	}

	want := &plan.Procedure{
		Spec: &functions.SchemaMutationProcedureSpec{
			Mutations: []functions.SchemaMutator{m1, m2, m3},
		},
	}
	plantest.PhysicalPlan_PushDown_TestHelper(t, spec, root, false, want)
}
*/
