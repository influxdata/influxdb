package compiler_test

import (
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/ifql/ast"
	"github.com/influxdata/ifql/compiler"
	"github.com/influxdata/ifql/semantic"
	"github.com/influxdata/ifql/semantic/semantictest"
	"github.com/influxdata/ifql/values"
)

var CmpOptions []cmp.Option

func init() {
	CmpOptions = append(semantictest.CmpOptions, cmp.Comparer(ValueEqual))
}

func ValueEqual(x, y values.Value) bool {
	if x.Type() != y.Type() {
		return false
	}
	switch k := x.Type().Kind(); k {
	case semantic.Bool:
		return x.Bool() == y.Bool()
	case semantic.UInt:
		return x.UInt() == y.UInt()
	case semantic.Int:
		return x.Int() == y.Int()
	case semantic.Float:
		return x.Float() == y.Float()
	case semantic.String:
		return x.Str() == y.Str()
	case semantic.Time:
		return x.Time() == y.Time()
	case semantic.Object:
		return cmp.Equal(x.Object(), y.Object(), CmpOptions...)
	default:
		return false
	}
}

func TestCompilationCache(t *testing.T) {
	add := &semantic.FunctionExpression{
		Params: []*semantic.FunctionParam{
			{Key: &semantic.Identifier{Name: "a"}},
			{Key: &semantic.Identifier{Name: "b"}},
		},
		Body: &semantic.BinaryExpression{
			Operator: ast.AdditionOperator,
			Left:     &semantic.IdentifierExpression{Name: "a"},
			Right:    &semantic.IdentifierExpression{Name: "b"},
		},
	}
	testCases := []struct {
		name  string
		types map[string]semantic.Type
		scope map[string]values.Value
		want  values.Value
	}{
		{
			name: "floats",
			types: map[string]semantic.Type{
				"a": semantic.Float,
				"b": semantic.Float,
			},
			scope: map[string]values.Value{
				"a": values.NewFloatValue(5),
				"b": values.NewFloatValue(4),
			},
			want: values.NewFloatValue(9),
		},
		{
			name: "ints",
			types: map[string]semantic.Type{
				"a": semantic.Int,
				"b": semantic.Int,
			},
			scope: map[string]values.Value{
				"a": values.NewIntValue(5),
				"b": values.NewIntValue(4),
			},
			want: values.NewIntValue(9),
		},
		{
			name: "uints",
			types: map[string]semantic.Type{
				"a": semantic.UInt,
				"b": semantic.UInt,
			},
			scope: map[string]values.Value{
				"a": values.NewUIntValue(5),
				"b": values.NewUIntValue(4),
			},
			want: values.NewUIntValue(9),
		},
	}

	//Reuse the same cache for all test cases
	cache := compiler.NewCompilationCache(add, nil, nil)
	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			f0, err := cache.Compile(tc.types)
			if err != nil {
				t.Fatal(err)
			}
			f1, err := cache.Compile(tc.types)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(f0, f1) {
				t.Errorf("unexpected new compilation result")
			}

			got0, err := f0.Eval(tc.scope)
			if err != nil {
				t.Fatal(err)
			}
			got1, err := f1.Eval(tc.scope)
			if err != nil {
				t.Fatal(err)
			}

			if !cmp.Equal(got0, tc.want, CmpOptions...) {
				t.Errorf("unexpected eval result -want/+got\n%s", cmp.Diff(tc.want, got0, CmpOptions...))
			}
			if !cmp.Equal(got0, got1, CmpOptions...) {
				t.Errorf("unexpected differing results -got0/+got1\n%s", cmp.Diff(got0, got1, CmpOptions...))
			}

		})
	}
}

func TestCompileAndEval(t *testing.T) {
	testCases := []struct {
		name    string
		fn      *semantic.FunctionExpression
		types   map[string]semantic.Type
		scope   map[string]values.Value
		want    values.Value
		wantErr bool
	}{
		{
			name: "simple ident return",
			fn: &semantic.FunctionExpression{
				Params: []*semantic.FunctionParam{
					{Key: &semantic.Identifier{Name: "r"}},
				},
				Body: &semantic.IdentifierExpression{Name: "r"},
			},
			types: map[string]semantic.Type{
				"r": semantic.Int,
			},
			scope: map[string]values.Value{
				"r": values.NewIntValue(4),
			},
			want:    values.NewIntValue(4),
			wantErr: false,
		},
		{
			name: "call function",
			fn: &semantic.FunctionExpression{
				Params: []*semantic.FunctionParam{
					{Key: &semantic.Identifier{Name: "r"}},
				},
				Body: &semantic.CallExpression{
					Callee: &semantic.FunctionExpression{
						Params: []*semantic.FunctionParam{
							{Key: &semantic.Identifier{Name: "a"}, Default: &semantic.IntegerLiteral{Value: 1}},
							{Key: &semantic.Identifier{Name: "b"}, Default: &semantic.IntegerLiteral{Value: 1}},
						},
						Body: &semantic.BinaryExpression{
							Operator: ast.AdditionOperator,
							Left:     &semantic.IdentifierExpression{Name: "a"},
							Right:    &semantic.IdentifierExpression{Name: "b"},
						},
					},
					Arguments: &semantic.ObjectExpression{
						Properties: []*semantic.Property{
							{Key: &semantic.Identifier{Name: "a"}, Value: &semantic.IntegerLiteral{Value: 1}},
							{Key: &semantic.Identifier{Name: "b"}, Value: &semantic.IdentifierExpression{Name: "r"}},
						},
					},
				},
			},
			types: map[string]semantic.Type{
				"r": semantic.Int,
			},
			scope: map[string]values.Value{
				"r": values.NewIntValue(4),
			},
			want:    values.NewIntValue(5),
			wantErr: false,
		},
		{
			name: "call function via identifier",
			fn: &semantic.FunctionExpression{
				Params: []*semantic.FunctionParam{
					{Key: &semantic.Identifier{Name: "r"}},
				},
				Body: &semantic.BlockStatement{
					Body: []semantic.Statement{
						&semantic.NativeVariableDeclaration{
							Identifier: &semantic.Identifier{Name: "f"}, Init: &semantic.FunctionExpression{
								Params: []*semantic.FunctionParam{
									{Key: &semantic.Identifier{Name: "a"}, Default: &semantic.IntegerLiteral{Value: 1}},
									{Key: &semantic.Identifier{Name: "b"}, Default: &semantic.IntegerLiteral{Value: 1}},
								},
								Body: &semantic.BinaryExpression{
									Operator: ast.AdditionOperator,
									Left:     &semantic.IdentifierExpression{Name: "a"},
									Right:    &semantic.IdentifierExpression{Name: "b"},
								},
							},
						},
						&semantic.ReturnStatement{
							Argument: &semantic.CallExpression{
								Callee: &semantic.IdentifierExpression{Name: "f"},
								Arguments: &semantic.ObjectExpression{
									Properties: []*semantic.Property{
										{Key: &semantic.Identifier{Name: "a"}, Value: &semantic.IntegerLiteral{Value: 1}},
										{Key: &semantic.Identifier{Name: "b"}, Value: &semantic.IdentifierExpression{Name: "r"}},
									},
								},
							},
						},
					},
				},
			},
			types: map[string]semantic.Type{
				"r": semantic.Int,
			},
			scope: map[string]values.Value{
				"r": values.NewIntValue(4),
			},
			want:    values.NewIntValue(5),
			wantErr: false,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			f, err := compiler.Compile(tc.fn, tc.types, nil, nil)
			if tc.wantErr != (err != nil) {
				t.Fatalf("unexpected error %s", err)
			}

			got, err := f.Eval(tc.scope)
			if tc.wantErr != (err != nil) {
				t.Errorf("unexpected error %s", err)
			}

			if !cmp.Equal(tc.want, got, CmpOptions...) {
				t.Errorf("unexpected value -want/+got\n%s", cmp.Diff(tc.want, got, CmpOptions...))
			}
		})
	}
}
