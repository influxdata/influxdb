package semantic_test

import (
	"encoding/json"
	"math"
	"regexp"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/ifql/ast"
	"github.com/influxdata/ifql/semantic"
	"github.com/influxdata/ifql/semantic/semantictest"
)

func TestJSONMarshal(t *testing.T) {
	testCases := []struct {
		name string
		node semantic.Node
		want string
	}{
		{
			name: "simple program",
			node: &semantic.Program{
				Body: []semantic.Statement{
					&semantic.ExpressionStatement{
						Expression: &semantic.StringLiteral{Value: "hello"},
					},
				},
			},
			want: `{"type":"Program","body":[{"type":"ExpressionStatement","expression":{"type":"StringLiteral","value":"hello"}}]}`,
		},
		{
			name: "block statement",
			node: &semantic.BlockStatement{
				Body: []semantic.Statement{
					&semantic.ExpressionStatement{
						Expression: &semantic.StringLiteral{Value: "hello"},
					},
				},
			},
			want: `{"type":"BlockStatement","body":[{"type":"ExpressionStatement","expression":{"type":"StringLiteral","value":"hello"}}]}`,
		},
		{
			name: "expression statement",
			node: &semantic.ExpressionStatement{
				Expression: &semantic.StringLiteral{Value: "hello"},
			},
			want: `{"type":"ExpressionStatement","expression":{"type":"StringLiteral","value":"hello"}}`,
		},
		{
			name: "return statement",
			node: &semantic.ReturnStatement{
				Argument: &semantic.StringLiteral{Value: "hello"},
			},
			want: `{"type":"ReturnStatement","argument":{"type":"StringLiteral","value":"hello"}}`,
		},
		{
			name: "variable declaration",
			node: &semantic.NativeVariableDeclaration{
				Identifier: &semantic.Identifier{Name: "a"},
				Init:       &semantic.StringLiteral{Value: "hello"},
			},
			want: `{"type":"NativeVariableDeclaration","identifier":{"type":"Identifier","name":"a"},"init":{"type":"StringLiteral","value":"hello"}}`,
		},
		{
			name: "call expression",
			node: &semantic.CallExpression{
				Callee:    &semantic.IdentifierExpression{Name: "a"},
				Arguments: &semantic.ObjectExpression{Properties: []*semantic.Property{{Key: &semantic.Identifier{Name: "s"}, Value: &semantic.StringLiteral{Value: "hello"}}}},
			},
			want: `{"type":"CallExpression","callee":{"type":"IdentifierExpression","name":"a"},"arguments":{"type":"ObjectExpression","properties":[{"type":"Property","key":{"type":"Identifier","name":"s"},"value":{"type":"StringLiteral","value":"hello"}}]}}`,
		},
		{
			name: "member expression",
			node: &semantic.MemberExpression{
				Object:   &semantic.IdentifierExpression{Name: "a"},
				Property: "hello",
			},
			want: `{"type":"MemberExpression","object":{"type":"IdentifierExpression","name":"a"},"property":"hello"}`,
		},
		{
			name: "arrow function expression",
			node: &semantic.FunctionExpression{
				Params: []*semantic.FunctionParam{{Key: &semantic.Identifier{Name: "a"}}},
				Body:   &semantic.StringLiteral{Value: "hello"},
			},
			want: `{"type":"ArrowFunctionExpression","params":[{"type":"FunctionParam","key":{"type":"Identifier","name":"a"},"default":null}],"body":{"type":"StringLiteral","value":"hello"}}`,
		},
		{
			name: "binary expression",
			node: &semantic.BinaryExpression{
				Operator: ast.AdditionOperator,
				Left:     &semantic.StringLiteral{Value: "hello"},
				Right:    &semantic.StringLiteral{Value: "world"},
			},
			want: `{"type":"BinaryExpression","operator":"+","left":{"type":"StringLiteral","value":"hello"},"right":{"type":"StringLiteral","value":"world"}}`,
		},
		{
			name: "unary expression",
			node: &semantic.UnaryExpression{
				Operator: ast.NotOperator,
				Argument: &semantic.BooleanLiteral{Value: true},
			},
			want: `{"type":"UnaryExpression","operator":"not","argument":{"type":"BooleanLiteral","value":true}}`,
		},
		{
			name: "logical expression",
			node: &semantic.LogicalExpression{
				Operator: ast.OrOperator,
				Left:     &semantic.BooleanLiteral{Value: false},
				Right:    &semantic.BooleanLiteral{Value: true},
			},
			want: `{"type":"LogicalExpression","operator":"or","left":{"type":"BooleanLiteral","value":false},"right":{"type":"BooleanLiteral","value":true}}`,
		},
		{
			name: "array expression",
			node: &semantic.ArrayExpression{
				Elements: []semantic.Expression{&semantic.StringLiteral{Value: "hello"}},
			},
			want: `{"type":"ArrayExpression","elements":[{"type":"StringLiteral","value":"hello"}]}`,
		},
		{
			name: "object expression",
			node: &semantic.ObjectExpression{
				Properties: []*semantic.Property{{
					Key:   &semantic.Identifier{Name: "a"},
					Value: &semantic.StringLiteral{Value: "hello"},
				}},
			},
			want: `{"type":"ObjectExpression","properties":[{"type":"Property","key":{"type":"Identifier","name":"a"},"value":{"type":"StringLiteral","value":"hello"}}]}`,
		},
		{
			name: "conditional expression",
			node: &semantic.ConditionalExpression{
				Test:       &semantic.BooleanLiteral{Value: true},
				Alternate:  &semantic.StringLiteral{Value: "false"},
				Consequent: &semantic.StringLiteral{Value: "true"},
			},
			want: `{"type":"ConditionalExpression","test":{"type":"BooleanLiteral","value":true},"alternate":{"type":"StringLiteral","value":"false"},"consequent":{"type":"StringLiteral","value":"true"}}`,
		},
		{
			name: "property",
			node: &semantic.Property{
				Key:   &semantic.Identifier{Name: "a"},
				Value: &semantic.StringLiteral{Value: "hello"},
			},
			want: `{"type":"Property","key":{"type":"Identifier","name":"a"},"value":{"type":"StringLiteral","value":"hello"}}`,
		},
		{
			name: "identifier",
			node: &semantic.Identifier{
				Name: "a",
			},
			want: `{"type":"Identifier","name":"a"}`,
		},
		{
			name: "string literal",
			node: &semantic.StringLiteral{
				Value: "hello",
			},
			want: `{"type":"StringLiteral","value":"hello"}`,
		},
		{
			name: "boolean literal",
			node: &semantic.BooleanLiteral{
				Value: true,
			},
			want: `{"type":"BooleanLiteral","value":true}`,
		},
		{
			name: "float literal",
			node: &semantic.FloatLiteral{
				Value: 42.1,
			},
			want: `{"type":"FloatLiteral","value":42.1}`,
		},
		{
			name: "integer literal",
			node: &semantic.IntegerLiteral{
				Value: math.MaxInt64,
			},
			want: `{"type":"IntegerLiteral","value":"9223372036854775807"}`,
		},
		{
			name: "unsigned integer literal",
			node: &semantic.UnsignedIntegerLiteral{
				Value: math.MaxUint64,
			},
			want: `{"type":"UnsignedIntegerLiteral","value":"18446744073709551615"}`,
		},
		{
			name: "regexp literal",
			node: &semantic.RegexpLiteral{
				Value: regexp.MustCompile(`.*`),
			},
			want: `{"type":"RegexpLiteral","value":".*"}`,
		},
		{
			name: "duration literal",
			node: &semantic.DurationLiteral{
				Value: time.Hour + time.Minute,
			},
			want: `{"type":"DurationLiteral","value":"1h1m0s"}`,
		},
		{
			name: "datetime literal",
			node: &semantic.DateTimeLiteral{
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
			node, err := semantic.UnmarshalNode(data)
			if err != nil {
				t.Fatal(err)
			}
			if !cmp.Equal(tc.node, node, semantictest.CmpOptions...) {
				t.Errorf("unexpected node after unmarshalling: -want/+got:\n%s", cmp.Diff(tc.node, node, semantictest.CmpOptions...))
			}
		})
	}
}
