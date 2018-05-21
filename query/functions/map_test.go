package functions_test

import (
	"testing"

	"github.com/influxdata/ifql/ast"
	"github.com/influxdata/ifql/functions"
	"github.com/influxdata/ifql/query"
	"github.com/influxdata/ifql/query/execute"
	"github.com/influxdata/ifql/query/execute/executetest"
	"github.com/influxdata/ifql/query/querytest"
	"github.com/influxdata/ifql/semantic"
)

func TestMap_NewQuery(t *testing.T) {
	tests := []querytest.NewQueryTestCase{
		{
			Name: "simple static map",
			Raw:  `from(db:"mydb") |> map(fn: (r) => r._value + 1)`,
			Want: &query.Spec{
				Operations: []*query.Operation{
					{
						ID: "from0",
						Spec: &functions.FromOpSpec{
							Database: "mydb",
						},
					},
					{
						ID: "map1",
						Spec: &functions.MapOpSpec{
							Fn: &semantic.FunctionExpression{
								Params: []*semantic.FunctionParam{{Key: &semantic.Identifier{Name: "r"}}},
								Body: &semantic.BinaryExpression{
									Operator: ast.AdditionOperator,
									Left: &semantic.MemberExpression{
										Object: &semantic.IdentifierExpression{
											Name: "r",
										},
										Property: "_value",
									},
									Right: &semantic.IntegerLiteral{Value: 1},
								},
							},
						},
					},
				},
				Edges: []query.Edge{
					{Parent: "from0", Child: "map1"},
				},
			},
		},
		{
			Name: "resolve map",
			Raw:  `x = 2 from(db:"mydb") |> map(fn: (r) => r._value + x)`,
			Want: &query.Spec{
				Operations: []*query.Operation{
					{
						ID: "from0",
						Spec: &functions.FromOpSpec{
							Database: "mydb",
						},
					},
					{
						ID: "map1",
						Spec: &functions.MapOpSpec{
							Fn: &semantic.FunctionExpression{
								Params: []*semantic.FunctionParam{{Key: &semantic.Identifier{Name: "r"}}},
								Body: &semantic.BinaryExpression{
									Operator: ast.AdditionOperator,
									Left: &semantic.MemberExpression{
										Object: &semantic.IdentifierExpression{
											Name: "r",
										},
										Property: "_value",
									},
									Right: &semantic.IntegerLiteral{Value: 2},
								},
							},
						},
					},
				},
				Edges: []query.Edge{
					{Parent: "from0", Child: "map1"},
				},
			},
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

func TestMapOperation_Marshaling(t *testing.T) {
	data := []byte(`{
		"id":"map",
		"kind":"map",
		"spec":{
			"fn":{
				"type": "ArrowFunctionExpression",
				"params": [{"type":"FunctionParam","key":{"type":"Identifier","name":"r"}}],
				"body":{
					"type":"BinaryExpression",
					"operator": "-",
					"left":{
						"type":"MemberExpression",
						"object": {
							"type": "IdentifierExpression",
							"name":"r"
						},
						"property": "_value"
					},
					"right":{
						"type":"FloatLiteral",
						"value": 5.6
					}
				}
			}
		}
	}`)
	op := &query.Operation{
		ID: "map",
		Spec: &functions.MapOpSpec{
			Fn: &semantic.FunctionExpression{
				Params: []*semantic.FunctionParam{{Key: &semantic.Identifier{Name: "r"}}},
				Body: &semantic.BinaryExpression{
					Operator: ast.SubtractionOperator,
					Left: &semantic.MemberExpression{
						Object: &semantic.IdentifierExpression{
							Name: "r",
						},
						Property: "_value",
					},
					Right: &semantic.FloatLiteral{Value: 5.6},
				},
			},
		},
	}
	querytest.OperationMarshalingTestHelper(t, data, op)
}
func TestMap_Process(t *testing.T) {
	testCases := []struct {
		name string
		spec *functions.MapProcedureSpec
		data []execute.Block
		want []*executetest.Block
	}{
		{
			name: `_value+5`,
			spec: &functions.MapProcedureSpec{
				Fn: &semantic.FunctionExpression{
					Params: []*semantic.FunctionParam{{Key: &semantic.Identifier{Name: "r"}}},
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
								Key: &semantic.Identifier{Name: "_value"},
								Value: &semantic.BinaryExpression{
									Operator: ast.AdditionOperator,
									Left: &semantic.MemberExpression{
										Object: &semantic.IdentifierExpression{
											Name: "r",
										},
										Property: "_value",
									},
									Right: &semantic.FloatLiteral{
										Value: 5,
									},
								},
							},
						},
					},
				},
			},
			data: []execute.Block{&executetest.Block{
				ColMeta: []execute.ColMeta{
					{Label: "_time", Type: execute.TTime},
					{Label: "_value", Type: execute.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(1), 1.0},
					{execute.Time(2), 6.0},
				},
			}},
			want: []*executetest.Block{{
				ColMeta: []execute.ColMeta{
					{Label: "_time", Type: execute.TTime},
					{Label: "_value", Type: execute.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(1), 6.0},
					{execute.Time(2), 11.0},
				},
			}},
		},
		{
			name: `_value*_value`,
			spec: &functions.MapProcedureSpec{
				Fn: &semantic.FunctionExpression{
					Params: []*semantic.FunctionParam{{Key: &semantic.Identifier{Name: "r"}}},
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
								Key: &semantic.Identifier{Name: "_value"},
								Value: &semantic.BinaryExpression{
									Operator: ast.MultiplicationOperator,
									Left: &semantic.MemberExpression{
										Object: &semantic.IdentifierExpression{
											Name: "r",
										},
										Property: "_value",
									},
									Right: &semantic.MemberExpression{
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
			},
			data: []execute.Block{&executetest.Block{
				ColMeta: []execute.ColMeta{
					{Label: "_time", Type: execute.TTime},
					{Label: "_value", Type: execute.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(1), 1.0},
					{execute.Time(2), 6.0},
				},
			}},
			want: []*executetest.Block{{
				ColMeta: []execute.ColMeta{
					{Label: "_time", Type: execute.TTime},
					{Label: "_value", Type: execute.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(1), 1.0},
					{execute.Time(2), 36.0},
				},
			}},
		},
		{
			name: "float(r._value) int",
			spec: &functions.MapProcedureSpec{
				Fn: &semantic.FunctionExpression{
					Params: []*semantic.FunctionParam{{Key: &semantic.Identifier{Name: "r"}}},
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
								Key: &semantic.Identifier{Name: "_value"},
								Value: &semantic.CallExpression{
									Callee: &semantic.IdentifierExpression{Name: "float"},
									Arguments: &semantic.ObjectExpression{
										Properties: []*semantic.Property{{
											Key: &semantic.Identifier{Name: "v"},
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
				},
			},
			data: []execute.Block{&executetest.Block{
				ColMeta: []execute.ColMeta{
					{Label: "_time", Type: execute.TTime},
					{Label: "_value", Type: execute.TInt},
				},
				Data: [][]interface{}{
					{execute.Time(1), int64(1)},
					{execute.Time(2), int64(6)},
				},
			}},
			want: []*executetest.Block{{
				ColMeta: []execute.ColMeta{
					{Label: "_time", Type: execute.TTime},
					{Label: "_value", Type: execute.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(1), 1.0},
					{execute.Time(2), 6.0},
				},
			}},
		},
		{
			name: "float(r._value) uint",
			spec: &functions.MapProcedureSpec{
				Fn: &semantic.FunctionExpression{
					Params: []*semantic.FunctionParam{{Key: &semantic.Identifier{Name: "r"}}},
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
								Key: &semantic.Identifier{Name: "_value"},
								Value: &semantic.CallExpression{
									Callee: &semantic.IdentifierExpression{Name: "float"},
									Arguments: &semantic.ObjectExpression{
										Properties: []*semantic.Property{{
											Key: &semantic.Identifier{Name: "v"},
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
				},
			},
			data: []execute.Block{&executetest.Block{
				ColMeta: []execute.ColMeta{
					{Label: "_time", Type: execute.TTime},
					{Label: "_value", Type: execute.TUInt},
				},
				Data: [][]interface{}{
					{execute.Time(1), uint64(1)},
					{execute.Time(2), uint64(6)},
				},
			}},
			want: []*executetest.Block{{
				ColMeta: []execute.ColMeta{
					{Label: "_time", Type: execute.TTime},
					{Label: "_value", Type: execute.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(1), 1.0},
					{execute.Time(2), 6.0},
				},
			}},
		},
	}
	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			executetest.ProcessTestHelper(
				t,
				tc.data,
				tc.want,
				func(d execute.Dataset, c execute.BlockBuilderCache) execute.Transformation {
					f, err := functions.NewMapTransformation(d, c, tc.spec)
					if err != nil {
						t.Fatal(err)
					}
					return f
				},
			)
		})
	}
}
