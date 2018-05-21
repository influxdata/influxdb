package parser_test

import (
	"regexp"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/platform/query/ast"
	"github.com/influxdata/platform/query/ast/asttest"
	"github.com/influxdata/platform/query/parser"
)

func TestParse(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    *ast.Program
		wantErr bool
	}{
		{
			name: "from",
			raw:  `from()`,
			want: &ast.Program{
				Body: []ast.Statement{
					&ast.ExpressionStatement{
						Expression: &ast.CallExpression{
							Callee: &ast.Identifier{
								Name: "from",
							},
						},
					},
				},
			},
		},
		{
			name: "comment",
			raw: `// Comment
			from()`,
			want: &ast.Program{
				Body: []ast.Statement{
					&ast.ExpressionStatement{
						Expression: &ast.CallExpression{
							Callee: &ast.Identifier{
								Name: "from",
							},
						},
					},
				},
			},
		},
		{
			name: "identifier with number",
			raw:  `tan2()`,
			want: &ast.Program{
				Body: []ast.Statement{
					&ast.ExpressionStatement{
						Expression: &ast.CallExpression{
							Callee: &ast.Identifier{
								Name: "tan2",
							},
						},
					},
				},
			},
		},
		{
			name: "regex literal",
			raw:  `/.*/`,
			want: &ast.Program{
				Body: []ast.Statement{
					&ast.ExpressionStatement{
						Expression: &ast.RegexpLiteral{
							Value: regexp.MustCompile(".*"),
						},
					},
				},
			},
		},
		{
			name: "regex literal with escape sequence",
			raw:  `/a\/b\\c\d/`,
			want: &ast.Program{
				Body: []ast.Statement{
					&ast.ExpressionStatement{
						Expression: &ast.RegexpLiteral{
							Value: regexp.MustCompile(`a/b\\c\d`),
						},
					},
				},
			},
		},
		{
			name: "regex match operators",
			raw:  `"a" =~ /.*/ and "b" !~ /c/`,
			want: &ast.Program{
				Body: []ast.Statement{
					&ast.ExpressionStatement{
						Expression: &ast.LogicalExpression{
							Operator: ast.AndOperator,
							Left: &ast.BinaryExpression{
								Operator: ast.RegexpMatchOperator,
								Left:     &ast.StringLiteral{Value: "a"},
								Right:    &ast.RegexpLiteral{Value: regexp.MustCompile(".*")},
							},
							Right: &ast.BinaryExpression{
								Operator: ast.NotRegexpMatchOperator,
								Left:     &ast.StringLiteral{Value: "b"},
								Right:    &ast.RegexpLiteral{Value: regexp.MustCompile("c")},
							},
						},
					},
				},
			},
		},
		{
			name: "declare variable as an int",
			raw:  `howdy = 1`,
			want: &ast.Program{
				Body: []ast.Statement{
					&ast.VariableDeclaration{
						Declarations: []*ast.VariableDeclarator{{
							ID:   &ast.Identifier{Name: "howdy"},
							Init: &ast.IntegerLiteral{Value: 1},
						}},
					},
				},
			},
		},
		{
			name: "declare variable as a float",
			raw:  `howdy = 1.1`,
			want: &ast.Program{
				Body: []ast.Statement{
					&ast.VariableDeclaration{
						Declarations: []*ast.VariableDeclarator{{
							ID:   &ast.Identifier{Name: "howdy"},
							Init: &ast.FloatLiteral{Value: 1.1},
						}},
					},
				},
			},
		},
		{
			name: "declare variable as an array",
			raw:  `howdy = [1, 2, 3, 4]`,
			want: &ast.Program{
				Body: []ast.Statement{
					&ast.VariableDeclaration{
						Declarations: []*ast.VariableDeclarator{{
							ID: &ast.Identifier{Name: "howdy"},
							Init: &ast.ArrayExpression{
								Elements: []ast.Expression{
									&ast.IntegerLiteral{Value: 1},
									&ast.IntegerLiteral{Value: 2},
									&ast.IntegerLiteral{Value: 3},
									&ast.IntegerLiteral{Value: 4},
								},
							},
						}},
					},
				},
			},
		},
		{
			name: "use variable to declare something",
			raw: `howdy = 1
			from()`,
			want: &ast.Program{
				Body: []ast.Statement{
					&ast.VariableDeclaration{
						Declarations: []*ast.VariableDeclarator{{
							ID:   &ast.Identifier{Name: "howdy"},
							Init: &ast.IntegerLiteral{Value: 1},
						}},
					},
					&ast.ExpressionStatement{
						Expression: &ast.CallExpression{
							Callee: &ast.Identifier{
								Name: "from",
							},
						},
					},
				},
			},
		},
		{
			name: "variable is from statement",
			raw: `howdy = from()
			howdy.count()`,
			want: &ast.Program{
				Body: []ast.Statement{
					&ast.VariableDeclaration{
						Declarations: []*ast.VariableDeclarator{{
							ID: &ast.Identifier{
								Name: "howdy",
							},
							Init: &ast.CallExpression{
								Callee: &ast.Identifier{
									Name: "from",
								},
							},
						}},
					},
					&ast.ExpressionStatement{
						Expression: &ast.CallExpression{
							Callee: &ast.MemberExpression{
								Object: &ast.Identifier{
									Name: "howdy",
								},
								Property: &ast.Identifier{
									Name: "count",
								},
							},
						},
					},
				},
			},
		},
		{
			name: "pipe expression",
			raw:  `from() |> count()`,
			want: &ast.Program{
				Body: []ast.Statement{
					&ast.ExpressionStatement{
						Expression: &ast.PipeExpression{
							Argument: &ast.CallExpression{
								Callee:    &ast.Identifier{Name: "from"},
								Arguments: nil,
							},
							Call: &ast.CallExpression{
								Callee:    &ast.Identifier{Name: "count"},
								Arguments: nil,
							},
						},
					},
				},
			},
		},
		{
			name: "literal pipe expression",
			raw:  `5 |> pow2()`,
			want: &ast.Program{
				Body: []ast.Statement{
					&ast.ExpressionStatement{
						Expression: &ast.PipeExpression{
							Argument: &ast.IntegerLiteral{Value: 5},
							Call: &ast.CallExpression{
								Callee:    &ast.Identifier{Name: "pow2"},
								Arguments: nil,
							},
						},
					},
				},
			},
		},
		{
			name: "member expression pipe expression",
			raw:  `foo.bar |> baz()`,
			want: &ast.Program{
				Body: []ast.Statement{
					&ast.ExpressionStatement{
						Expression: &ast.PipeExpression{
							Argument: &ast.MemberExpression{
								Object:   &ast.Identifier{Name: "foo"},
								Property: &ast.Identifier{Name: "bar"},
							},
							Call: &ast.CallExpression{
								Callee:    &ast.Identifier{Name: "baz"},
								Arguments: nil,
							},
						},
					},
				},
			},
		},
		{
			name: "multiple pipe expressions",
			raw:  `from() |> range() |> filter() |> count()`,
			want: &ast.Program{
				Body: []ast.Statement{
					&ast.ExpressionStatement{
						Expression: &ast.PipeExpression{
							Argument: &ast.PipeExpression{
								Argument: &ast.PipeExpression{
									Argument: &ast.CallExpression{
										Callee: &ast.Identifier{Name: "from"},
									},
									Call: &ast.CallExpression{
										Callee: &ast.Identifier{Name: "range"},
									},
								},
								Call: &ast.CallExpression{
									Callee: &ast.Identifier{Name: "filter"},
								},
							},
							Call: &ast.CallExpression{
								Callee: &ast.Identifier{Name: "count"},
							},
						},
					},
				},
			},
		},
		{
			name: "two variables for two froms",
			raw: `howdy = from()
			doody = from()
			howdy|>count()
			doody|>sum()`,
			want: &ast.Program{
				Body: []ast.Statement{
					&ast.VariableDeclaration{
						Declarations: []*ast.VariableDeclarator{{
							ID: &ast.Identifier{
								Name: "howdy",
							},
							Init: &ast.CallExpression{
								Callee: &ast.Identifier{
									Name: "from",
								},
							},
						}},
					},
					&ast.VariableDeclaration{
						Declarations: []*ast.VariableDeclarator{{
							ID: &ast.Identifier{
								Name: "doody",
							},
							Init: &ast.CallExpression{
								Callee: &ast.Identifier{
									Name: "from",
								},
							},
						}},
					},
					&ast.ExpressionStatement{
						Expression: &ast.PipeExpression{
							Argument: &ast.Identifier{Name: "howdy"},
							Call: &ast.CallExpression{
								Callee: &ast.Identifier{
									Name: "count",
								},
							},
						},
					},
					&ast.ExpressionStatement{
						Expression: &ast.PipeExpression{
							Argument: &ast.Identifier{Name: "doody"},
							Call: &ast.CallExpression{
								Callee: &ast.Identifier{
									Name: "sum",
								},
							},
						},
					},
				},
			},
		},
		{
			name: "from with database",
			raw:  `from(db:"telegraf")`,
			want: &ast.Program{
				Body: []ast.Statement{
					&ast.ExpressionStatement{
						Expression: &ast.CallExpression{
							Callee: &ast.Identifier{
								Name: "from",
							},
							Arguments: []ast.Expression{
								&ast.ObjectExpression{
									Properties: []*ast.Property{
										{
											Key: &ast.Identifier{
												Name: "db",
											},
											Value: &ast.StringLiteral{
												Value: "telegraf",
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "map member expressions",
			raw: `m = {key1: 1, key2:"value2"}
			m.key1
			m["key2"]
			`,
			want: &ast.Program{
				Body: []ast.Statement{
					&ast.VariableDeclaration{
						Declarations: []*ast.VariableDeclarator{{
							ID: &ast.Identifier{
								Name: "m",
							},
							Init: &ast.ObjectExpression{
								Properties: []*ast.Property{
									{
										Key:   &ast.Identifier{Name: "key1"},
										Value: &ast.IntegerLiteral{Value: 1},
									},
									{
										Key:   &ast.Identifier{Name: "key2"},
										Value: &ast.StringLiteral{Value: "value2"},
									},
								},
							},
						}},
					},
					&ast.ExpressionStatement{
						Expression: &ast.MemberExpression{
							Object:   &ast.Identifier{Name: "m"},
							Property: &ast.Identifier{Name: "key1"},
						},
					},
					&ast.ExpressionStatement{
						Expression: &ast.MemberExpression{
							Object:   &ast.Identifier{Name: "m"},
							Property: &ast.StringLiteral{Value: "key2"},
						},
					},
				},
			},
		},
		{
			name: "var as binary expression of other vars",
			raw: `a = 1
            b = 2
            c = a + b
            d = a`,
			want: &ast.Program{
				Body: []ast.Statement{
					&ast.VariableDeclaration{
						Declarations: []*ast.VariableDeclarator{{
							ID: &ast.Identifier{
								Name: "a",
							},
							Init: &ast.IntegerLiteral{Value: 1},
						}},
					},
					&ast.VariableDeclaration{
						Declarations: []*ast.VariableDeclarator{{
							ID: &ast.Identifier{
								Name: "b",
							},
							Init: &ast.IntegerLiteral{Value: 2},
						}},
					},
					&ast.VariableDeclaration{
						Declarations: []*ast.VariableDeclarator{{
							ID: &ast.Identifier{
								Name: "c",
							},
							Init: &ast.BinaryExpression{
								Operator: ast.AdditionOperator,
								Left:     &ast.Identifier{Name: "a"},
								Right:    &ast.Identifier{Name: "b"},
							},
						}},
					},
					&ast.VariableDeclaration{
						Declarations: []*ast.VariableDeclarator{{
							ID: &ast.Identifier{
								Name: "d",
							},
							Init: &ast.Identifier{Name: "a"},
						}},
					},
				},
			},
		},
		{
			name: "var as unary expression of other vars",
			raw: `a = 5
            c = -a`,
			want: &ast.Program{
				Body: []ast.Statement{
					&ast.VariableDeclaration{
						Declarations: []*ast.VariableDeclarator{{
							ID: &ast.Identifier{
								Name: "a",
							},
							Init: &ast.IntegerLiteral{Value: 5},
						}},
					},
					&ast.VariableDeclaration{
						Declarations: []*ast.VariableDeclarator{{
							ID: &ast.Identifier{
								Name: "c",
							},
							Init: &ast.UnaryExpression{
								Operator: ast.SubtractionOperator,
								Argument: &ast.Identifier{Name: "a"},
							},
						}},
					},
				},
			},
		},
		{
			name: "var as both binary and unary expressions",
			raw: `a = 5
            c = 10 * -a`,
			want: &ast.Program{
				Body: []ast.Statement{
					&ast.VariableDeclaration{
						Declarations: []*ast.VariableDeclarator{{
							ID: &ast.Identifier{
								Name: "a",
							},
							Init: &ast.IntegerLiteral{Value: 5},
						}},
					},
					&ast.VariableDeclaration{
						Declarations: []*ast.VariableDeclarator{{
							ID: &ast.Identifier{
								Name: "c",
							},
							Init: &ast.BinaryExpression{
								Operator: ast.MultiplicationOperator,
								Left:     &ast.IntegerLiteral{Value: 10},
								Right: &ast.UnaryExpression{
									Operator: ast.SubtractionOperator,
									Argument: &ast.Identifier{Name: "a"},
								},
							},
						}},
					},
				},
			},
		},
		{
			name: "unary expressions within logical expression",
			raw: `a = 5.0
            10.0 * -a == -0.5 or a == 6.0`,
			want: &ast.Program{
				Body: []ast.Statement{
					&ast.VariableDeclaration{
						Declarations: []*ast.VariableDeclarator{{
							ID: &ast.Identifier{
								Name: "a",
							},
							Init: &ast.FloatLiteral{Value: 5},
						}},
					},
					&ast.ExpressionStatement{
						Expression: &ast.LogicalExpression{
							Operator: ast.OrOperator,
							Left: &ast.BinaryExpression{
								Operator: ast.EqualOperator,
								Left: &ast.BinaryExpression{
									Operator: ast.MultiplicationOperator,
									Left:     &ast.FloatLiteral{Value: 10},
									Right: &ast.UnaryExpression{
										Operator: ast.SubtractionOperator,
										Argument: &ast.Identifier{Name: "a"},
									},
								},
								Right: &ast.UnaryExpression{
									Operator: ast.SubtractionOperator,
									Argument: &ast.FloatLiteral{Value: 0.5},
								},
							},
							Right: &ast.BinaryExpression{
								Operator: ast.EqualOperator,
								Left:     &ast.Identifier{Name: "a"},
								Right:    &ast.FloatLiteral{Value: 6},
							},
						},
					},
				},
			},
		},
		{
			name: "unary expressions with too many comments",
			raw: `// define a
a = 5.0
// eval this
10.0 * -a == -0.5
	// or this
	or a == 6.0`,
			want: &ast.Program{
				Body: []ast.Statement{
					&ast.VariableDeclaration{
						Declarations: []*ast.VariableDeclarator{{
							ID: &ast.Identifier{
								Name: "a",
							},
							Init: &ast.FloatLiteral{Value: 5},
						}},
					},
					&ast.ExpressionStatement{
						Expression: &ast.LogicalExpression{
							Operator: ast.OrOperator,
							Left: &ast.BinaryExpression{
								Operator: ast.EqualOperator,
								Left: &ast.BinaryExpression{
									Operator: ast.MultiplicationOperator,
									Left:     &ast.FloatLiteral{Value: 10},
									Right: &ast.UnaryExpression{
										Operator: ast.SubtractionOperator,
										Argument: &ast.Identifier{Name: "a"},
									},
								},
								Right: &ast.UnaryExpression{
									Operator: ast.SubtractionOperator,
									Argument: &ast.FloatLiteral{Value: 0.5},
								},
							},
							Right: &ast.BinaryExpression{
								Operator: ast.EqualOperator,
								Left:     &ast.Identifier{Name: "a"},
								Right:    &ast.FloatLiteral{Value: 6},
							},
						},
					},
				},
			},
		},
		{
			name: "expressions with function calls",
			raw:  `a = foo() == 10`,
			want: &ast.Program{
				Body: []ast.Statement{
					&ast.VariableDeclaration{
						Declarations: []*ast.VariableDeclarator{{
							ID: &ast.Identifier{
								Name: "a",
							},
							Init: &ast.BinaryExpression{
								Operator: ast.EqualOperator,
								Left: &ast.CallExpression{
									Callee: &ast.Identifier{Name: "foo"},
								},
								Right: &ast.IntegerLiteral{Value: 10},
							},
						}},
					},
				},
			},
		},
		{
			name: "mix unary logical and binary expressions",
			raw: `
            not (f() == 6.0 * x) or fail()`,
			want: &ast.Program{
				Body: []ast.Statement{
					&ast.ExpressionStatement{
						Expression: &ast.LogicalExpression{
							Operator: ast.OrOperator,
							Left: &ast.UnaryExpression{
								Operator: ast.NotOperator,
								Argument: &ast.BinaryExpression{
									Operator: ast.EqualOperator,
									Left: &ast.CallExpression{
										Callee: &ast.Identifier{Name: "f"},
									},
									Right: &ast.BinaryExpression{
										Operator: ast.MultiplicationOperator,
										Left:     &ast.FloatLiteral{Value: 6},
										Right:    &ast.Identifier{Name: "x"},
									},
								},
							},
							Right: &ast.CallExpression{
								Callee: &ast.Identifier{Name: "fail"},
							},
						},
					},
				},
			},
		},
		{
			name: "mix unary logical and binary expressions with extra parens",
			raw: `
            (not (f() == 6.0 * x) or fail())`,
			want: &ast.Program{
				Body: []ast.Statement{
					&ast.ExpressionStatement{
						Expression: &ast.LogicalExpression{
							Operator: ast.OrOperator,
							Left: &ast.UnaryExpression{
								Operator: ast.NotOperator,
								Argument: &ast.BinaryExpression{
									Operator: ast.EqualOperator,
									Left: &ast.CallExpression{
										Callee: &ast.Identifier{Name: "f"},
									},
									Right: &ast.BinaryExpression{
										Operator: ast.MultiplicationOperator,
										Left:     &ast.FloatLiteral{Value: 6},
										Right:    &ast.Identifier{Name: "x"},
									},
								},
							},
							Right: &ast.CallExpression{
								Callee: &ast.Identifier{Name: "fail"},
							},
						},
					},
				},
			},
		},
		{
			name: "arrow function called",
			raw: `plusOne = (r) => r + 1
			plusOne(r:5)
			`,
			want: &ast.Program{
				Body: []ast.Statement{
					&ast.VariableDeclaration{
						Declarations: []*ast.VariableDeclarator{{
							ID: &ast.Identifier{
								Name: "plusOne",
							},
							Init: &ast.ArrowFunctionExpression{
								Params: []*ast.Property{{Key: &ast.Identifier{Name: "r"}}},
								Body: &ast.BinaryExpression{
									Operator: ast.AdditionOperator,
									Left:     &ast.Identifier{Name: "r"},
									Right:    &ast.IntegerLiteral{Value: 1},
								},
							},
						}},
					},
					&ast.ExpressionStatement{
						Expression: &ast.CallExpression{
							Callee: &ast.Identifier{Name: "plusOne"},
							Arguments: []ast.Expression{
								&ast.ObjectExpression{
									Properties: []*ast.Property{
										{
											Key: &ast.Identifier{
												Name: "r",
											},
											Value: &ast.IntegerLiteral{
												Value: 5,
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "arrow function return map",
			raw:  `toMap = (r) =>({r:r})`,
			want: &ast.Program{
				Body: []ast.Statement{
					&ast.VariableDeclaration{
						Declarations: []*ast.VariableDeclarator{{
							ID: &ast.Identifier{
								Name: "toMap",
							},
							Init: &ast.ArrowFunctionExpression{
								Params: []*ast.Property{{Key: &ast.Identifier{Name: "r"}}},
								Body: &ast.ObjectExpression{
									Properties: []*ast.Property{{
										Key:   &ast.Identifier{Name: "r"},
										Value: &ast.Identifier{Name: "r"},
									}},
								},
							},
						}},
					},
				},
			},
		},
		{
			name: "arrow function with default arg",
			raw:  `addN = (r, n=5) => r + n`,
			want: &ast.Program{
				Body: []ast.Statement{
					&ast.VariableDeclaration{
						Declarations: []*ast.VariableDeclarator{{
							ID: &ast.Identifier{
								Name: "addN",
							},
							Init: &ast.ArrowFunctionExpression{
								Params: []*ast.Property{
									{Key: &ast.Identifier{Name: "r"}},
									{Key: &ast.Identifier{Name: "n"}, Value: &ast.IntegerLiteral{Value: 5}},
								},
								Body: &ast.BinaryExpression{
									Operator: ast.AdditionOperator,
									Left:     &ast.Identifier{Name: "r"},
									Right:    &ast.Identifier{Name: "n"},
								},
							},
						}},
					},
				},
			},
		},
		{
			name: "arrow function called in binary expression",
			raw: `
            plusOne = (r) => r + 1
            plusOne(r:5) == 6 or die()
			`,
			want: &ast.Program{
				Body: []ast.Statement{
					&ast.VariableDeclaration{
						Declarations: []*ast.VariableDeclarator{{
							ID: &ast.Identifier{
								Name: "plusOne",
							},
							Init: &ast.ArrowFunctionExpression{
								Params: []*ast.Property{{Key: &ast.Identifier{Name: "r"}}},
								Body: &ast.BinaryExpression{
									Operator: ast.AdditionOperator,
									Left:     &ast.Identifier{Name: "r"},
									Right:    &ast.IntegerLiteral{Value: 1},
								},
							},
						}},
					},
					&ast.ExpressionStatement{
						Expression: &ast.LogicalExpression{
							Operator: ast.OrOperator,
							Left: &ast.BinaryExpression{
								Operator: ast.EqualOperator,
								Left: &ast.CallExpression{
									Callee: &ast.Identifier{Name: "plusOne"},
									Arguments: []ast.Expression{
										&ast.ObjectExpression{
											Properties: []*ast.Property{
												{
													Key: &ast.Identifier{
														Name: "r",
													},
													Value: &ast.IntegerLiteral{
														Value: 5,
													},
												},
											},
										},
									},
								},
								Right: &ast.IntegerLiteral{Value: 6},
							},
							Right: &ast.CallExpression{
								Callee: &ast.Identifier{Name: "die"},
							},
						},
					},
				},
			},
		},
		{
			name: "arrow function as single expression",
			raw:  `f = (r) => r["_measurement"] == "cpu"`,
			want: &ast.Program{
				Body: []ast.Statement{
					&ast.VariableDeclaration{
						Declarations: []*ast.VariableDeclarator{{
							ID: &ast.Identifier{
								Name: "f",
							},
							Init: &ast.ArrowFunctionExpression{
								Params: []*ast.Property{{Key: &ast.Identifier{Name: "r"}}},
								Body: &ast.BinaryExpression{
									Operator: ast.EqualOperator,
									Left: &ast.MemberExpression{
										Object:   &ast.Identifier{Name: "r"},
										Property: &ast.StringLiteral{Value: "_measurement"},
									},
									Right: &ast.StringLiteral{Value: "cpu"},
								},
							},
						}},
					},
				},
			},
		},
		{
			name: "arrow function as block",
			raw: `f = (r) => { 
                m = r["_measurement"]
                return m == "cpu"
            }`,
			want: &ast.Program{
				Body: []ast.Statement{
					&ast.VariableDeclaration{
						Declarations: []*ast.VariableDeclarator{{
							ID: &ast.Identifier{
								Name: "f",
							},
							Init: &ast.ArrowFunctionExpression{
								Params: []*ast.Property{{Key: &ast.Identifier{Name: "r"}}},
								Body: &ast.BlockStatement{
									Body: []ast.Statement{
										&ast.VariableDeclaration{
											Declarations: []*ast.VariableDeclarator{{
												ID: &ast.Identifier{
													Name: "m",
												},
												Init: &ast.MemberExpression{
													Object:   &ast.Identifier{Name: "r"},
													Property: &ast.StringLiteral{Value: "_measurement"},
												},
											}},
										},
										&ast.ReturnStatement{
											Argument: &ast.BinaryExpression{
												Operator: ast.EqualOperator,
												Left:     &ast.Identifier{Name: "m"},
												Right:    &ast.StringLiteral{Value: "cpu"},
											},
										},
									},
								},
							},
						}},
					},
				},
			},
		},
		{
			name: "from with filter with no parens",
			raw:  `from(db:"telegraf").filter(fn: (r) => r["other"]=="mem" and r["this"]=="that" or r["these"]!="those")`,
			want: &ast.Program{
				Body: []ast.Statement{
					&ast.ExpressionStatement{
						Expression: &ast.CallExpression{
							Callee: &ast.MemberExpression{
								Property: &ast.Identifier{Name: "filter"},
								Object: &ast.CallExpression{
									Callee: &ast.Identifier{
										Name: "from",
									},
									Arguments: []ast.Expression{
										&ast.ObjectExpression{
											Properties: []*ast.Property{
												{
													Key:   &ast.Identifier{Name: "db"},
													Value: &ast.StringLiteral{Value: "telegraf"},
												},
											},
										},
									},
								},
							},
							Arguments: []ast.Expression{
								&ast.ObjectExpression{
									Properties: []*ast.Property{
										{
											Key: &ast.Identifier{Name: "fn"},
											Value: &ast.ArrowFunctionExpression{
												Params: []*ast.Property{{Key: &ast.Identifier{Name: "r"}}},
												Body: &ast.LogicalExpression{
													Operator: ast.OrOperator,
													Left: &ast.LogicalExpression{
														Operator: ast.AndOperator,
														Left: &ast.BinaryExpression{
															Operator: ast.EqualOperator,
															Left: &ast.MemberExpression{
																Object:   &ast.Identifier{Name: "r"},
																Property: &ast.StringLiteral{Value: "other"},
															},
															Right: &ast.StringLiteral{Value: "mem"},
														},
														Right: &ast.BinaryExpression{
															Operator: ast.EqualOperator,
															Left: &ast.MemberExpression{
																Object:   &ast.Identifier{Name: "r"},
																Property: &ast.StringLiteral{Value: "this"},
															},
															Right: &ast.StringLiteral{Value: "that"},
														},
													},
													Right: &ast.BinaryExpression{
														Operator: ast.NotEqualOperator,
														Left: &ast.MemberExpression{
															Object:   &ast.Identifier{Name: "r"},
															Property: &ast.StringLiteral{Value: "these"},
														},
														Right: &ast.StringLiteral{Value: "those"},
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "from with range",
			raw:  `from(db:"telegraf")|>range(start:-1h, end:10m)`,
			want: &ast.Program{
				Body: []ast.Statement{
					&ast.ExpressionStatement{
						Expression: &ast.PipeExpression{
							Argument: &ast.CallExpression{
								Callee: &ast.Identifier{Name: "from"},
								Arguments: []ast.Expression{
									&ast.ObjectExpression{
										Properties: []*ast.Property{
											{
												Key:   &ast.Identifier{Name: "db"},
												Value: &ast.StringLiteral{Value: "telegraf"},
											},
										},
									},
								},
							},
							Call: &ast.CallExpression{
								Callee: &ast.Identifier{Name: "range"},
								Arguments: []ast.Expression{
									&ast.ObjectExpression{
										Properties: []*ast.Property{
											{
												Key: &ast.Identifier{Name: "start"},
												Value: &ast.UnaryExpression{
													Operator: ast.SubtractionOperator,
													Argument: &ast.DurationLiteral{Value: time.Hour},
												},
											},
											{
												Key:   &ast.Identifier{Name: "end"},
												Value: &ast.DurationLiteral{Value: 10 * time.Minute},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "from with limit",
			raw:  `from(db:"telegraf")|>limit(limit:100, offset:10)`,
			want: &ast.Program{
				Body: []ast.Statement{
					&ast.ExpressionStatement{
						Expression: &ast.PipeExpression{
							Argument: &ast.CallExpression{
								Callee: &ast.Identifier{Name: "from"},
								Arguments: []ast.Expression{
									&ast.ObjectExpression{
										Properties: []*ast.Property{
											{
												Key:   &ast.Identifier{Name: "db"},
												Value: &ast.StringLiteral{Value: "telegraf"},
											},
										},
									},
								},
							},
							Call: &ast.CallExpression{
								Callee: &ast.Identifier{Name: "limit"},
								Arguments: []ast.Expression{
									&ast.ObjectExpression{
										Properties: []*ast.Property{
											{
												Key:   &ast.Identifier{Name: "limit"},
												Value: &ast.IntegerLiteral{Value: 100},
											},
											{
												Key:   &ast.Identifier{Name: "offset"},
												Value: &ast.IntegerLiteral{Value: 10},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "from with range and count",
			raw: `from(db:"mydb")
						|> range(start:-4h, stop:-2h)
						|> count()`,
			want: &ast.Program{
				Body: []ast.Statement{
					&ast.ExpressionStatement{
						Expression: &ast.PipeExpression{
							Argument: &ast.PipeExpression{
								Argument: &ast.CallExpression{
									Callee: &ast.Identifier{Name: "from"},
									Arguments: []ast.Expression{
										&ast.ObjectExpression{
											Properties: []*ast.Property{
												{
													Key:   &ast.Identifier{Name: "db"},
													Value: &ast.StringLiteral{Value: "mydb"},
												},
											},
										},
									},
								},
								Call: &ast.CallExpression{
									Callee: &ast.Identifier{Name: "range"},
									Arguments: []ast.Expression{
										&ast.ObjectExpression{
											Properties: []*ast.Property{
												{
													Key: &ast.Identifier{Name: "start"},
													Value: &ast.UnaryExpression{
														Operator: ast.SubtractionOperator,
														Argument: &ast.DurationLiteral{Value: 4 * time.Hour},
													},
												},
												{
													Key: &ast.Identifier{Name: "stop"},
													Value: &ast.UnaryExpression{
														Operator: ast.SubtractionOperator,
														Argument: &ast.DurationLiteral{Value: 2 * time.Hour},
													},
												},
											},
										},
									},
								},
							},
							Call: &ast.CallExpression{
								Callee: &ast.Identifier{Name: "count"},
							},
						},
					},
				},
			},
		},
		{
			name: "from with range, limit and count",
			raw: `from(db:"mydb")
						|> range(start:-4h, stop:-2h)
						|> limit(n:10)
						|> count()`,
			want: &ast.Program{
				Body: []ast.Statement{
					&ast.ExpressionStatement{
						Expression: &ast.PipeExpression{
							Argument: &ast.PipeExpression{
								Argument: &ast.PipeExpression{
									Argument: &ast.CallExpression{
										Callee: &ast.Identifier{Name: "from"},
										Arguments: []ast.Expression{
											&ast.ObjectExpression{
												Properties: []*ast.Property{
													{
														Key:   &ast.Identifier{Name: "db"},
														Value: &ast.StringLiteral{Value: "mydb"},
													},
												},
											},
										},
									},
									Call: &ast.CallExpression{
										Callee: &ast.Identifier{Name: "range"},
										Arguments: []ast.Expression{
											&ast.ObjectExpression{
												Properties: []*ast.Property{
													{
														Key: &ast.Identifier{Name: "start"},
														Value: &ast.UnaryExpression{
															Operator: ast.SubtractionOperator,
															Argument: &ast.DurationLiteral{Value: 4 * time.Hour},
														},
													},
													{
														Key: &ast.Identifier{Name: "stop"},
														Value: &ast.UnaryExpression{
															Operator: ast.SubtractionOperator,
															Argument: &ast.DurationLiteral{Value: 2 * time.Hour},
														},
													},
												},
											},
										},
									},
								},
								Call: &ast.CallExpression{
									Callee: &ast.Identifier{Name: "limit"},
									Arguments: []ast.Expression{
										&ast.ObjectExpression{
											Properties: []*ast.Property{{
												Key:   &ast.Identifier{Name: "n"},
												Value: &ast.IntegerLiteral{Value: 10},
											}},
										},
									},
								},
							},
							Call: &ast.CallExpression{
								Callee: &ast.Identifier{Name: "count"},
							},
						},
					},
				},
			},
		},
		{
			name: "from with join",
			raw: `
a = from(db:"dbA") |> range(start:-1h)
b = from(db:"dbB") |> range(start:-1h)
join(tables:[a,b], on:["host"], fn: (a,b) => a["_field"] + b["_field"])`,
			want: &ast.Program{
				Body: []ast.Statement{
					&ast.VariableDeclaration{
						Declarations: []*ast.VariableDeclarator{{
							ID: &ast.Identifier{
								Name: "a",
							},
							Init: &ast.PipeExpression{
								Argument: &ast.CallExpression{
									Callee: &ast.Identifier{Name: "from"},
									Arguments: []ast.Expression{
										&ast.ObjectExpression{
											Properties: []*ast.Property{
												{
													Key:   &ast.Identifier{Name: "db"},
													Value: &ast.StringLiteral{Value: "dbA"},
												},
											},
										},
									},
								},
								Call: &ast.CallExpression{
									Callee: &ast.Identifier{Name: "range"},
									Arguments: []ast.Expression{
										&ast.ObjectExpression{
											Properties: []*ast.Property{
												{
													Key: &ast.Identifier{Name: "start"},
													Value: &ast.UnaryExpression{
														Operator: ast.SubtractionOperator,
														Argument: &ast.DurationLiteral{Value: 1 * time.Hour},
													},
												},
											},
										},
									},
								},
							},
						}},
					},
					&ast.VariableDeclaration{
						Declarations: []*ast.VariableDeclarator{{
							ID: &ast.Identifier{
								Name: "b",
							},
							Init: &ast.PipeExpression{
								Argument: &ast.CallExpression{
									Callee: &ast.Identifier{Name: "from"},
									Arguments: []ast.Expression{
										&ast.ObjectExpression{
											Properties: []*ast.Property{
												{
													Key:   &ast.Identifier{Name: "db"},
													Value: &ast.StringLiteral{Value: "dbB"},
												},
											},
										},
									},
								},
								Call: &ast.CallExpression{
									Callee: &ast.Identifier{Name: "range"},
									Arguments: []ast.Expression{
										&ast.ObjectExpression{
											Properties: []*ast.Property{
												{
													Key: &ast.Identifier{Name: "start"},
													Value: &ast.UnaryExpression{
														Operator: ast.SubtractionOperator,
														Argument: &ast.DurationLiteral{Value: 1 * time.Hour},
													},
												},
											},
										},
									},
								},
							},
						}},
					},
					&ast.ExpressionStatement{
						Expression: &ast.CallExpression{
							Callee: &ast.Identifier{Name: "join"},
							Arguments: []ast.Expression{
								&ast.ObjectExpression{
									Properties: []*ast.Property{
										{
											Key: &ast.Identifier{Name: "tables"},
											Value: &ast.ArrayExpression{
												Elements: []ast.Expression{
													&ast.Identifier{Name: "a"},
													&ast.Identifier{Name: "b"},
												},
											},
										},
										{
											Key: &ast.Identifier{Name: "on"},
											Value: &ast.ArrayExpression{
												Elements: []ast.Expression{&ast.StringLiteral{Value: "host"}},
											},
										},
										{
											Key: &ast.Identifier{Name: "fn"},
											Value: &ast.ArrowFunctionExpression{
												Params: []*ast.Property{
													{Key: &ast.Identifier{Name: "a"}},
													{Key: &ast.Identifier{Name: "b"}},
												},
												Body: &ast.BinaryExpression{
													Operator: ast.AdditionOperator,
													Left: &ast.MemberExpression{
														Object:   &ast.Identifier{Name: "a"},
														Property: &ast.StringLiteral{Value: "_field"},
													},
													Right: &ast.MemberExpression{
														Object:   &ast.Identifier{Name: "b"},
														Property: &ast.StringLiteral{Value: "_field"},
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "from with join with complex expression",
			raw: `
a = from(db:"ifql")
	|> filter(fn: (r) => r["_measurement"] == "a")
	|> range(start:-1h)

b = from(db:"ifql")
	|> filter(fn: (r) => r["_measurement"] == "b")
	|> range(start:-1h)

join(tables:[a,b], on:["t1"], fn: (a,b) => (a["_field"] - b["_field"]) / b["_field"])
`,
			want: &ast.Program{
				Body: []ast.Statement{
					&ast.VariableDeclaration{
						Declarations: []*ast.VariableDeclarator{{
							ID: &ast.Identifier{
								Name: "a",
							},
							Init: &ast.PipeExpression{
								Argument: &ast.PipeExpression{
									Argument: &ast.CallExpression{
										Callee: &ast.Identifier{Name: "from"},
										Arguments: []ast.Expression{
											&ast.ObjectExpression{
												Properties: []*ast.Property{
													{
														Key:   &ast.Identifier{Name: "db"},
														Value: &ast.StringLiteral{Value: "ifql"},
													},
												},
											},
										},
									},
									Call: &ast.CallExpression{
										Callee: &ast.Identifier{Name: "filter"},
										Arguments: []ast.Expression{
											&ast.ObjectExpression{
												Properties: []*ast.Property{
													{
														Key: &ast.Identifier{Name: "fn"},
														Value: &ast.ArrowFunctionExpression{
															Params: []*ast.Property{{Key: &ast.Identifier{Name: "r"}}},
															Body: &ast.BinaryExpression{
																Operator: ast.EqualOperator,
																Left: &ast.MemberExpression{
																	Object:   &ast.Identifier{Name: "r"},
																	Property: &ast.StringLiteral{Value: "_measurement"},
																},
																Right: &ast.StringLiteral{Value: "a"},
															},
														},
													},
												},
											},
										},
									},
								},
								Call: &ast.CallExpression{
									Callee: &ast.Identifier{Name: "range"},
									Arguments: []ast.Expression{
										&ast.ObjectExpression{
											Properties: []*ast.Property{
												{
													Key: &ast.Identifier{Name: "start"},
													Value: &ast.UnaryExpression{
														Operator: ast.SubtractionOperator,
														Argument: &ast.DurationLiteral{Value: 1 * time.Hour},
													},
												},
											},
										},
									},
								},
							},
						}},
					},
					&ast.VariableDeclaration{
						Declarations: []*ast.VariableDeclarator{{
							ID: &ast.Identifier{
								Name: "b",
							},
							Init: &ast.PipeExpression{
								Argument: &ast.PipeExpression{
									Argument: &ast.CallExpression{
										Callee: &ast.Identifier{Name: "from"},
										Arguments: []ast.Expression{
											&ast.ObjectExpression{
												Properties: []*ast.Property{
													{
														Key:   &ast.Identifier{Name: "db"},
														Value: &ast.StringLiteral{Value: "ifql"},
													},
												},
											},
										},
									},
									Call: &ast.CallExpression{
										Callee: &ast.Identifier{Name: "filter"},
										Arguments: []ast.Expression{
											&ast.ObjectExpression{
												Properties: []*ast.Property{
													{
														Key: &ast.Identifier{Name: "fn"},
														Value: &ast.ArrowFunctionExpression{
															Params: []*ast.Property{{Key: &ast.Identifier{Name: "r"}}},
															Body: &ast.BinaryExpression{
																Operator: ast.EqualOperator,
																Left: &ast.MemberExpression{
																	Object:   &ast.Identifier{Name: "r"},
																	Property: &ast.StringLiteral{Value: "_measurement"},
																},
																Right: &ast.StringLiteral{Value: "b"},
															},
														},
													},
												},
											},
										},
									},
								},
								Call: &ast.CallExpression{
									Callee: &ast.Identifier{Name: "range"},
									Arguments: []ast.Expression{
										&ast.ObjectExpression{
											Properties: []*ast.Property{
												{
													Key: &ast.Identifier{Name: "start"},
													Value: &ast.UnaryExpression{
														Operator: ast.SubtractionOperator,
														Argument: &ast.DurationLiteral{Value: 1 * time.Hour},
													},
												},
											},
										},
									},
								},
							},
						}},
					},
					&ast.ExpressionStatement{
						Expression: &ast.CallExpression{
							Callee: &ast.Identifier{Name: "join"},
							Arguments: []ast.Expression{
								&ast.ObjectExpression{
									Properties: []*ast.Property{
										{
											Key: &ast.Identifier{Name: "tables"},
											Value: &ast.ArrayExpression{
												Elements: []ast.Expression{
													&ast.Identifier{Name: "a"},
													&ast.Identifier{Name: "b"},
												},
											},
										},
										{
											Key: &ast.Identifier{Name: "on"},
											Value: &ast.ArrayExpression{
												Elements: []ast.Expression{
													&ast.StringLiteral{
														Value: "t1",
													},
												},
											},
										},
										{
											Key: &ast.Identifier{Name: "fn"},
											Value: &ast.ArrowFunctionExpression{
												Params: []*ast.Property{
													{Key: &ast.Identifier{Name: "a"}},
													{Key: &ast.Identifier{Name: "b"}},
												},
												Body: &ast.BinaryExpression{
													Operator: ast.DivisionOperator,
													Left: &ast.BinaryExpression{
														Operator: ast.SubtractionOperator,
														Left: &ast.MemberExpression{
															Object:   &ast.Identifier{Name: "a"},
															Property: &ast.StringLiteral{Value: "_field"},
														},
														Right: &ast.MemberExpression{
															Object:   &ast.Identifier{Name: "b"},
															Property: &ast.StringLiteral{Value: "_field"},
														},
													},
													Right: &ast.MemberExpression{
														Object:   &ast.Identifier{Name: "b"},
														Property: &ast.StringLiteral{Value: "_field"},
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name:    "parse error extra gibberish",
			raw:     `from(db:"ifql") &^*&H#IUJBN`,
			wantErr: true,
		},
		{
			name:    "parse error extra gibberish and valid content",
			raw:     `from(db:"ifql") &^*&H#IUJBN from(db:"other")`,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			// Set the env var`GO_TAGS=parser_debug` in order
			// to turn on parser debugging as it is turned off by default.
			got, err := parser.NewAST(tt.raw)
			if (err != nil) != tt.wantErr {
				t.Errorf("ifql.NewAST() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}
			if !cmp.Equal(tt.want, got, asttest.CompareOptions...) {
				t.Errorf("ifql.NewAST() = -want/+got %s", cmp.Diff(tt.want, got, asttest.CompareOptions...))
			}
		})
	}
}

var benchmarkQuery = []byte(`
start = -10s

do = (cpu) =>
    from(db:"telegraf")
        .filter(fn: (r) =>
             r["_measurement"] == "cpu"
             and
             r["cpu"] == cpu)
        .range(start:start)

cpu0 = do(cpu:"cpu0")
cpu1 = do(cpu:"cpu1")

join(
    tables:[cpu0, cpu1],
    on:["_measurement","_field","host"],
    fn: (a,b) => a["_value"] - b["_value"],
)
`)

var benchmarkProgram interface{}

func BenchmarkParse(b *testing.B) {
	b.ReportAllocs()
	var err error
	for n := 0; n < b.N; n++ {
		benchmarkProgram, err = parser.Parse("", benchmarkQuery)
		if err != nil {
			b.Fatal(err)
		}
	}
}
