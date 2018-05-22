package execute_test

//func TestCompileExpression(t *testing.T) {
//	testCases := []struct {
//		name    string
//		expr    expression.Expression
//		types   map[string]execute.DataType
//		wantErr bool
//	}{
//		{
//			name: "integer literal",
//			expr: expression.Expression{
//				Root: &expression.IntegerLiteralNode{
//					Value: 42,
//				},
//			},
//			wantErr: false,
//		},
//		{
//			name: "negate string",
//			expr: expression.Expression{
//				Root: &expression.UnaryNode{
//					Operator: expression.SubtractionOperator,
//					Node: &expression.StringLiteralNode{
//						Value: "hello",
//					},
//				},
//			},
//			wantErr: true,
//		},
//		{
//			name: "missing type info",
//			expr: expression.Expression{
//				Root: &expression.ReferenceNode{
//					Name: "a",
//				},
//			},
//			wantErr: true,
//		},
//	}
//	for _, tc := range testCases {
//		tc := tc
//		t.Run(tc.name, func(t *testing.T) {
//			_, err := execute.CompileExpression(tc.expr, tc.types)
//			if err != nil {
//				if !tc.wantErr {
//					t.Errorf("unexpected compliation error: %s", err)
//				}
//			} else if tc.wantErr {
//				t.Error("expected compliation error")
//			}
//		})
//	}
//}
//func TestEvaluateCompiledExpression(t *testing.T) {
//	testCases := []struct {
//		name    string
//		expr    expression.Expression
//		types   map[string]execute.DataType
//		scope   execute.Scope
//		want    execute.Value
//		wantErr bool
//	}{
//		{
//			name: "integer literal",
//			expr: expression.Expression{
//				Root: &expression.IntegerLiteralNode{
//					Value: 42,
//				},
//			},
//			want: execute.Value{
//				Type:  execute.TInt,
//				Value: int64(42),
//			},
//		},
//		{
//			name: "integer addition",
//			expr: expression.Expression{
//				Root: &expression.BinaryNode{
//					Operator: expression.AdditionOperator,
//					Left: &expression.IntegerLiteralNode{
//						Value: 18,
//					},
//					Right: &expression.IntegerLiteralNode{
//						Value: 24,
//					},
//				},
//			},
//			want: execute.Value{
//				Type:  execute.TInt,
//				Value: int64(42),
//			},
//		},
//		{
//			name: "integer addition using scope",
//			expr: expression.Expression{
//				Root: &expression.BinaryNode{
//					Operator: expression.AdditionOperator,
//					Left: &expression.ReferenceNode{
//						Name: "a",
//					},
//					Right: &expression.ReferenceNode{
//						Name: "b",
//					},
//				},
//			},
//			types: map[string]execute.DataType{
//				"a": execute.TInt,
//				"b": execute.TInt,
//			},
//			scope: map[string]execute.Value{
//				"a": {Type: execute.TInt, Value: int64(18)},
//				"b": {Type: execute.TInt, Value: int64(24)},
//			},
//			want: execute.Value{
//				Type:  execute.TInt,
//				Value: int64(42),
//			},
//		},
//		{
//			name: "integer addition missing scope",
//			expr: expression.Expression{
//				Root: &expression.BinaryNode{
//					Operator: expression.AdditionOperator,
//					Left: &expression.ReferenceNode{
//						Name: "a",
//					},
//					Right: &expression.ReferenceNode{
//						Name: "b",
//					},
//				},
//			},
//			types: map[string]execute.DataType{
//				"a": execute.TInt,
//				"b": execute.TInt,
//			},
//			scope: map[string]execute.Value{
//				"a": {Type: execute.TInt, Value: int64(18)},
//			},
//			wantErr: true,
//		},
//		{
//			name: "integer addition incorrect scope",
//			expr: expression.Expression{
//				Root: &expression.BinaryNode{
//					Operator: expression.AdditionOperator,
//					Left: &expression.ReferenceNode{
//						Name: "a",
//					},
//					Right: &expression.ReferenceNode{
//						Name: "b",
//					},
//				},
//			},
//			types: map[string]execute.DataType{
//				"a": execute.TInt,
//				"b": execute.TInt,
//			},
//			scope: map[string]execute.Value{
//				"a": {Type: execute.TInt, Value: int64(18)},
//				"b": {Type: execute.TFloat, Value: float64(18)},
//			},
//			wantErr: true,
//		},
//		{
//			name: "unsigned integer addition",
//			expr: expression.Expression{
//				Root: &expression.BinaryNode{
//					Operator: expression.AdditionOperator,
//					Left: &expression.ReferenceNode{
//						Name: "a",
//					},
//					Right: &expression.ReferenceNode{
//						Name: "b",
//					},
//				},
//			},
//			types: map[string]execute.DataType{
//				"a": execute.TUInt,
//				"b": execute.TUInt,
//			},
//			scope: map[string]execute.Value{
//				"a": {Type: execute.TUInt, Value: uint64(18)},
//				"b": {Type: execute.TUInt, Value: uint64(24)},
//			},
//			want: execute.Value{
//				Type:  execute.TUInt,
//				Value: uint64(42),
//			},
//		},
//		{
//			name: "float addition",
//			expr: expression.Expression{
//				Root: &expression.BinaryNode{
//					Operator: expression.AdditionOperator,
//					Left: &expression.FloatLiteralNode{
//						Value: 18,
//					},
//					Right: &expression.FloatLiteralNode{
//						Value: 24,
//					},
//				},
//			},
//			want: execute.Value{
//				Type:  execute.TFloat,
//				Value: float64(42),
//			},
//		},
//		{
//			name: "boolean and",
//			expr: expression.Expression{
//				Root: &expression.BinaryNode{
//					Operator: expression.AndOperator,
//					Left: &expression.BooleanLiteralNode{
//						Value: true,
//					},
//					Right: &expression.BooleanLiteralNode{
//						Value: true,
//					},
//				},
//			},
//			want: execute.Value{
//				Type:  execute.TBool,
//				Value: true,
//			},
//		},
//		{
//			name: "boolean or",
//			expr: expression.Expression{
//				Root: &expression.BinaryNode{
//					Operator: expression.OrOperator,
//					Left: &expression.BooleanLiteralNode{
//						Value: false,
//					},
//					Right: &expression.BooleanLiteralNode{
//						Value: true,
//					},
//				},
//			},
//			want: execute.Value{
//				Type:  execute.TBool,
//				Value: true,
//			},
//		},
//	}
//	for _, tc := range testCases {
//		tc := tc
//		t.Run(tc.name, func(t *testing.T) {
//			ce, err := execute.CompileExpression(tc.expr, tc.types)
//			if err != nil {
//				t.Fatal(err)
//			}
//			got, err := ce.Eval(tc.scope)
//			if err != nil {
//				if !tc.wantErr {
//					t.Fatal(err)
//				}
//			} else if tc.wantErr {
//				t.Fatal("expected evaluation error")
//			}
//			if !cmp.Equal(got, tc.want) {
//				t.Errorf("unexpected value: -want/+got\n%s", cmp.Diff(tc.want, got))
//			}
//		})
//	}
//}
