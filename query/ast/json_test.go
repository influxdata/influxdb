package ast_test

import (
	"encoding/json"
	"math"
	"regexp"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/platform/query/ast"
	"github.com/influxdata/platform/query/ast/asttest"
)

func TestJSONMarshal(t *testing.T) {
	testCases := []struct {
		name string
		node ast.Node
		want string
	}{
		{
			name: "simple program",
			node: &ast.Program{
				Body: []ast.Statement{
					&ast.ExpressionStatement{
						Expression: &ast.StringLiteral{Value: "hello"},
					},
				},
			},
			want: `{"type":"Program","body":[{"type":"ExpressionStatement","expression":{"type":"StringLiteral","value":"hello"}}]}`,
		},
		{
			name: "block statement",
			node: &ast.BlockStatement{
				Body: []ast.Statement{
					&ast.ExpressionStatement{
						Expression: &ast.StringLiteral{Value: "hello"},
					},
				},
			},
			want: `{"type":"BlockStatement","body":[{"type":"ExpressionStatement","expression":{"type":"StringLiteral","value":"hello"}}]}`,
		},
		{
			name: "expression statement",
			node: &ast.ExpressionStatement{
				Expression: &ast.StringLiteral{Value: "hello"},
			},
			want: `{"type":"ExpressionStatement","expression":{"type":"StringLiteral","value":"hello"}}`,
		},
		{
			name: "return statement",
			node: &ast.ReturnStatement{
				Argument: &ast.StringLiteral{Value: "hello"},
			},
			want: `{"type":"ReturnStatement","argument":{"type":"StringLiteral","value":"hello"}}`,
		},
		{
			name: "variable declaration",
			node: &ast.VariableDeclaration{
				Declarations: []*ast.VariableDeclarator{
					{
						ID:   &ast.Identifier{Name: "a"},
						Init: &ast.StringLiteral{Value: "hello"},
					},
				},
			},
			want: `{"type":"VariableDeclaration","declarations":[{"type":"VariableDeclarator","id":{"type":"Identifier","name":"a"},"init":{"type":"StringLiteral","value":"hello"}}]}`,
		},
		{
			name: "variable declarator",
			node: &ast.VariableDeclarator{
				ID:   &ast.Identifier{Name: "a"},
				Init: &ast.StringLiteral{Value: "hello"},
			},
			want: `{"type":"VariableDeclarator","id":{"type":"Identifier","name":"a"},"init":{"type":"StringLiteral","value":"hello"}}`,
		},
		{
			name: "call expression",
			node: &ast.CallExpression{
				Callee:    &ast.Identifier{Name: "a"},
				Arguments: []ast.Expression{&ast.StringLiteral{Value: "hello"}},
			},
			want: `{"type":"CallExpression","callee":{"type":"Identifier","name":"a"},"arguments":[{"type":"StringLiteral","value":"hello"}]}`,
		},
		{
			name: "pipe expression",
			node: &ast.PipeExpression{
				Argument: &ast.Identifier{Name: "a"},
				Call: &ast.CallExpression{
					Callee:    &ast.Identifier{Name: "a"},
					Arguments: []ast.Expression{&ast.StringLiteral{Value: "hello"}},
				},
			},
			want: `{"type":"PipeExpression","argument":{"type":"Identifier","name":"a"},"call":{"type":"CallExpression","callee":{"type":"Identifier","name":"a"},"arguments":[{"type":"StringLiteral","value":"hello"}]}}`,
		},
		{
			name: "member expression",
			node: &ast.MemberExpression{
				Object:   &ast.Identifier{Name: "a"},
				Property: &ast.StringLiteral{Value: "hello"},
			},
			want: `{"type":"MemberExpression","object":{"type":"Identifier","name":"a"},"property":{"type":"StringLiteral","value":"hello"}}`,
		},
		{
			name: "arrow function expression",
			node: &ast.ArrowFunctionExpression{
				Params: []*ast.Property{{Key: &ast.Identifier{Name: "a"}}},
				Body:   &ast.StringLiteral{Value: "hello"},
			},
			want: `{"type":"ArrowFunctionExpression","params":[{"type":"Property","key":{"type":"Identifier","name":"a"},"value":null}],"body":{"type":"StringLiteral","value":"hello"}}`,
		},
		{
			name: "binary expression",
			node: &ast.BinaryExpression{
				Operator: ast.AdditionOperator,
				Left:     &ast.StringLiteral{Value: "hello"},
				Right:    &ast.StringLiteral{Value: "world"},
			},
			want: `{"type":"BinaryExpression","operator":"+","left":{"type":"StringLiteral","value":"hello"},"right":{"type":"StringLiteral","value":"world"}}`,
		},
		{
			name: "unary expression",
			node: &ast.UnaryExpression{
				Operator: ast.NotOperator,
				Argument: &ast.BooleanLiteral{Value: true},
			},
			want: `{"type":"UnaryExpression","operator":"not","argument":{"type":"BooleanLiteral","value":true}}`,
		},
		{
			name: "logical expression",
			node: &ast.LogicalExpression{
				Operator: ast.OrOperator,
				Left:     &ast.BooleanLiteral{Value: false},
				Right:    &ast.BooleanLiteral{Value: true},
			},
			want: `{"type":"LogicalExpression","operator":"or","left":{"type":"BooleanLiteral","value":false},"right":{"type":"BooleanLiteral","value":true}}`,
		},
		{
			name: "array expression",
			node: &ast.ArrayExpression{
				Elements: []ast.Expression{&ast.StringLiteral{Value: "hello"}},
			},
			want: `{"type":"ArrayExpression","elements":[{"type":"StringLiteral","value":"hello"}]}`,
		},
		{
			name: "object expression",
			node: &ast.ObjectExpression{
				Properties: []*ast.Property{{
					Key:   &ast.Identifier{Name: "a"},
					Value: &ast.StringLiteral{Value: "hello"},
				}},
			},
			want: `{"type":"ObjectExpression","properties":[{"type":"Property","key":{"type":"Identifier","name":"a"},"value":{"type":"StringLiteral","value":"hello"}}]}`,
		},
		{
			name: "conditional expression",
			node: &ast.ConditionalExpression{
				Test:       &ast.BooleanLiteral{Value: true},
				Alternate:  &ast.StringLiteral{Value: "false"},
				Consequent: &ast.StringLiteral{Value: "true"},
			},
			want: `{"type":"ConditionalExpression","test":{"type":"BooleanLiteral","value":true},"alternate":{"type":"StringLiteral","value":"false"},"consequent":{"type":"StringLiteral","value":"true"}}`,
		},
		{
			name: "property",
			node: &ast.Property{
				Key:   &ast.Identifier{Name: "a"},
				Value: &ast.StringLiteral{Value: "hello"},
			},
			want: `{"type":"Property","key":{"type":"Identifier","name":"a"},"value":{"type":"StringLiteral","value":"hello"}}`,
		},
		{
			name: "identifier",
			node: &ast.Identifier{
				Name: "a",
			},
			want: `{"type":"Identifier","name":"a"}`,
		},
		{
			name: "string literal",
			node: &ast.StringLiteral{
				Value: "hello",
			},
			want: `{"type":"StringLiteral","value":"hello"}`,
		},
		{
			name: "boolean literal",
			node: &ast.BooleanLiteral{
				Value: true,
			},
			want: `{"type":"BooleanLiteral","value":true}`,
		},
		{
			name: "float literal",
			node: &ast.FloatLiteral{
				Value: 42.1,
			},
			want: `{"type":"FloatLiteral","value":42.1}`,
		},
		{
			name: "integer literal",
			node: &ast.IntegerLiteral{
				Value: math.MaxInt64,
			},
			want: `{"type":"IntegerLiteral","value":"9223372036854775807"}`,
		},
		{
			name: "unsigned integer literal",
			node: &ast.UnsignedIntegerLiteral{
				Value: math.MaxUint64,
			},
			want: `{"type":"UnsignedIntegerLiteral","value":"18446744073709551615"}`,
		},
		{
			name: "regexp literal",
			node: &ast.RegexpLiteral{
				Value: regexp.MustCompile(`.*`),
			},
			want: `{"type":"RegexpLiteral","value":".*"}`,
		},
		{
			name: "duration literal",
			node: &ast.DurationLiteral{
				Value: time.Hour + time.Minute,
			},
			want: `{"type":"DurationLiteral","value":"1h1m0s"}`,
		},
		{
			name: "datetime literal",
			node: &ast.DateTimeLiteral{
				Value: time.Date(2017, 8, 8, 8, 8, 8, 8, time.UTC),
			},
			want: `{"type":"DateTimeLiteral","value":"2017-08-08T08:08:08.000000008Z"}`,
		},
	}
	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			data, err := json.Marshal(tc.node)
			if err != nil {
				t.Fatal(err)
			}
			if got := string(data); got != tc.want {
				t.Errorf("unexpected json data:\nwant:%s\ngot: %s\n", tc.want, got)
			}
			node, err := ast.UnmarshalNode(data)
			if err != nil {
				t.Fatal(err)
			}
			if !cmp.Equal(tc.node, node, asttest.CompareOptions...) {
				t.Errorf("unexpected node after unmarshalling: -want/+got:\n%s", cmp.Diff(tc.node, node, asttest.CompareOptions...))
			}
		})
	}
}
