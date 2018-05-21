package execute

//func TestBinaryFuncs(t *testing.T) {
//	testCases := []struct {
//		op     expression.Operator
//		l, r   interface{}
//		want   interface{}
//		noFunc bool
//	}{
//		{op: expression.AdditionOperator, l: int64(6), r: int64(7), want: int64(13)},
//		{op: expression.AdditionOperator, l: int64(6), r: uint64(7), noFunc: true},
//		{op: expression.AdditionOperator, l: int64(6), r: float64(7), noFunc: true},
//		{op: expression.AdditionOperator, l: int64(6), r: bool(false), noFunc: true},
//		{op: expression.AdditionOperator, l: uint64(6), r: int64(7), noFunc: true},
//		{op: expression.AdditionOperator, l: uint64(6), r: uint64(7), want: uint64(13)},
//		{op: expression.AdditionOperator, l: uint64(6), r: float64(7), noFunc: true},
//		{op: expression.AdditionOperator, l: uint64(6), r: bool(false), noFunc: true},
//		{op: expression.AdditionOperator, l: float64(6), r: int64(7), noFunc: true},
//		{op: expression.AdditionOperator, l: float64(6), r: uint64(7), noFunc: true},
//		{op: expression.AdditionOperator, l: float64(6), r: float64(7), want: float64(13)},
//		{op: expression.AdditionOperator, l: float64(6), r: bool(false), noFunc: true},
//		{op: expression.AdditionOperator, l: bool(true), r: int64(7), noFunc: true},
//		{op: expression.AdditionOperator, l: bool(true), r: uint64(7), noFunc: true},
//		{op: expression.AdditionOperator, l: bool(true), r: float64(7), noFunc: true},
//		{op: expression.AdditionOperator, l: bool(true), r: bool(false), noFunc: true},
//		{op: expression.SubtractionOperator, l: int64(6), r: int64(7), want: int64(-1)},
//		{op: expression.SubtractionOperator, l: int64(6), r: uint64(7), noFunc: true},
//		{op: expression.SubtractionOperator, l: int64(6), r: float64(7), noFunc: true},
//		{op: expression.SubtractionOperator, l: int64(6), r: bool(false), noFunc: true},
//		{op: expression.SubtractionOperator, l: uint64(6), r: int64(7), noFunc: true},
//		{op: expression.SubtractionOperator, l: uint64(7), r: uint64(6), want: uint64(1)},
//		{op: expression.SubtractionOperator, l: uint64(6), r: float64(7), noFunc: true},
//		{op: expression.SubtractionOperator, l: uint64(6), r: bool(false), noFunc: true},
//		{op: expression.SubtractionOperator, l: float64(6), r: int64(7), noFunc: true},
//		{op: expression.SubtractionOperator, l: float64(6), r: uint64(7), noFunc: true},
//		{op: expression.SubtractionOperator, l: float64(6), r: float64(7), want: float64(-1)},
//		{op: expression.SubtractionOperator, l: float64(6), r: bool(false), noFunc: true},
//		{op: expression.SubtractionOperator, l: bool(true), r: int64(7), noFunc: true},
//		{op: expression.SubtractionOperator, l: bool(true), r: uint64(7), noFunc: true},
//		{op: expression.SubtractionOperator, l: bool(true), r: float64(7), noFunc: true},
//		{op: expression.SubtractionOperator, l: bool(true), r: bool(false), noFunc: true},
//		{op: expression.MultiplicationOperator, l: int64(6), r: int64(7), want: int64(42)},
//		{op: expression.MultiplicationOperator, l: int64(6), r: uint64(7), noFunc: true},
//		{op: expression.MultiplicationOperator, l: int64(6), r: float64(7), noFunc: true},
//		{op: expression.MultiplicationOperator, l: int64(6), r: bool(false), noFunc: true},
//		{op: expression.MultiplicationOperator, l: uint64(6), r: int64(7), noFunc: true},
//		{op: expression.MultiplicationOperator, l: uint64(6), r: uint64(7), want: uint64(42)},
//		{op: expression.MultiplicationOperator, l: uint64(6), r: float64(7), noFunc: true},
//		{op: expression.MultiplicationOperator, l: uint64(6), r: bool(false), noFunc: true},
//		{op: expression.MultiplicationOperator, l: float64(6), r: int64(7), noFunc: true},
//		{op: expression.MultiplicationOperator, l: float64(6), r: uint64(7), noFunc: true},
//		{op: expression.MultiplicationOperator, l: float64(6), r: float64(7), want: float64(42)},
//		{op: expression.MultiplicationOperator, l: float64(6), r: bool(false), noFunc: true},
//		{op: expression.MultiplicationOperator, l: bool(true), r: int64(7), noFunc: true},
//		{op: expression.MultiplicationOperator, l: bool(true), r: uint64(7), noFunc: true},
//		{op: expression.MultiplicationOperator, l: bool(true), r: float64(7), noFunc: true},
//		{op: expression.MultiplicationOperator, l: bool(true), r: bool(false), noFunc: true},
//		{op: expression.DivisionOperator, l: int64(6), r: int64(3), want: int64(2)},
//		{op: expression.DivisionOperator, l: int64(6), r: uint64(7), noFunc: true},
//		{op: expression.DivisionOperator, l: int64(6), r: float64(7), noFunc: true},
//		{op: expression.DivisionOperator, l: int64(6), r: bool(false), noFunc: true},
//		{op: expression.DivisionOperator, l: uint64(6), r: int64(7), noFunc: true},
//		{op: expression.DivisionOperator, l: uint64(6), r: uint64(2), want: uint64(3)},
//		{op: expression.DivisionOperator, l: uint64(6), r: float64(7), noFunc: true},
//		{op: expression.DivisionOperator, l: uint64(6), r: bool(false), noFunc: true},
//		{op: expression.DivisionOperator, l: float64(6), r: int64(7), noFunc: true},
//		{op: expression.DivisionOperator, l: float64(6), r: uint64(7), noFunc: true},
//		{op: expression.DivisionOperator, l: float64(6), r: float64(7), want: float64(6.0 / 7.0)},
//		{op: expression.DivisionOperator, l: float64(6), r: bool(false), noFunc: true},
//		{op: expression.DivisionOperator, l: bool(true), r: int64(7), noFunc: true},
//		{op: expression.DivisionOperator, l: bool(true), r: uint64(7), noFunc: true},
//		{op: expression.DivisionOperator, l: bool(true), r: float64(7), noFunc: true},
//		{op: expression.DivisionOperator, l: bool(true), r: bool(false), noFunc: true},
//		{op: expression.LessThanEqualOperator, l: int64(6), r: int64(7), want: true},
//		{op: expression.LessThanEqualOperator, l: int64(6), r: uint64(7), want: true},
//		{op: expression.LessThanEqualOperator, l: int64(6), r: float64(7), want: true},
//		{op: expression.LessThanEqualOperator, l: int64(6), r: bool(false), noFunc: true},
//		{op: expression.LessThanEqualOperator, l: uint64(6), r: int64(7), want: true},
//		{op: expression.LessThanEqualOperator, l: uint64(6), r: uint64(7), want: true},
//		{op: expression.LessThanEqualOperator, l: uint64(6), r: float64(7), want: true},
//		{op: expression.LessThanEqualOperator, l: uint64(6), r: bool(false), noFunc: true},
//		{op: expression.LessThanEqualOperator, l: float64(6), r: int64(7), want: true},
//		{op: expression.LessThanEqualOperator, l: float64(6), r: uint64(7), want: true},
//		{op: expression.LessThanEqualOperator, l: float64(6), r: float64(7), want: true},
//		{op: expression.LessThanEqualOperator, l: float64(6), r: bool(false), noFunc: true},
//		{op: expression.LessThanEqualOperator, l: bool(true), r: int64(7), noFunc: true},
//		{op: expression.LessThanEqualOperator, l: bool(true), r: uint64(7), noFunc: true},
//		{op: expression.LessThanEqualOperator, l: bool(true), r: float64(7), noFunc: true},
//		{op: expression.LessThanEqualOperator, l: bool(true), r: bool(false), noFunc: true},
//		{op: expression.LessThanOperator, l: int64(6), r: int64(7), want: true},
//		{op: expression.LessThanOperator, l: int64(6), r: uint64(7), want: true},
//		{op: expression.LessThanOperator, l: int64(6), r: float64(7), want: true},
//		{op: expression.LessThanOperator, l: int64(6), r: bool(false), noFunc: true},
//		{op: expression.LessThanOperator, l: uint64(6), r: int64(7), want: true},
//		{op: expression.LessThanOperator, l: uint64(6), r: uint64(7), want: true},
//		{op: expression.LessThanOperator, l: uint64(6), r: float64(7), want: true},
//		{op: expression.LessThanOperator, l: uint64(6), r: bool(false), noFunc: true},
//		{op: expression.LessThanOperator, l: float64(6), r: int64(7), want: true},
//		{op: expression.LessThanOperator, l: float64(6), r: uint64(7), want: true},
//		{op: expression.LessThanOperator, l: float64(6), r: float64(7), want: true},
//		{op: expression.LessThanOperator, l: float64(6), r: bool(false), noFunc: true},
//		{op: expression.LessThanOperator, l: bool(true), r: int64(7), noFunc: true},
//		{op: expression.LessThanOperator, l: bool(true), r: uint64(7), noFunc: true},
//		{op: expression.LessThanOperator, l: bool(true), r: float64(7), noFunc: true},
//		{op: expression.LessThanOperator, l: bool(true), r: bool(false), noFunc: true},
//		{op: expression.GreaterThanEqualOperator, l: int64(6), r: int64(7), want: false},
//		{op: expression.GreaterThanEqualOperator, l: int64(6), r: uint64(7), want: false},
//		{op: expression.GreaterThanEqualOperator, l: int64(6), r: float64(7), want: false},
//		{op: expression.GreaterThanEqualOperator, l: int64(6), r: bool(false), noFunc: true},
//		{op: expression.GreaterThanEqualOperator, l: uint64(6), r: int64(7), want: false},
//		{op: expression.GreaterThanEqualOperator, l: uint64(6), r: uint64(7), want: false},
//		{op: expression.GreaterThanEqualOperator, l: uint64(6), r: float64(7), want: false},
//		{op: expression.GreaterThanEqualOperator, l: uint64(6), r: bool(false), noFunc: true},
//		{op: expression.GreaterThanEqualOperator, l: float64(6), r: int64(7), want: false},
//		{op: expression.GreaterThanEqualOperator, l: float64(6), r: uint64(7), want: false},
//		{op: expression.GreaterThanEqualOperator, l: float64(6), r: float64(7), want: false},
//		{op: expression.GreaterThanEqualOperator, l: float64(6), r: bool(false), noFunc: true},
//		{op: expression.GreaterThanEqualOperator, l: bool(true), r: int64(7), noFunc: true},
//		{op: expression.GreaterThanEqualOperator, l: bool(true), r: uint64(7), noFunc: true},
//		{op: expression.GreaterThanEqualOperator, l: bool(true), r: float64(7), noFunc: true},
//		{op: expression.GreaterThanEqualOperator, l: bool(true), r: bool(false), noFunc: true},
//		{op: expression.GreaterThanOperator, l: int64(6), r: int64(7), want: false},
//		{op: expression.GreaterThanOperator, l: int64(6), r: uint64(7), want: false},
//		{op: expression.GreaterThanOperator, l: int64(6), r: float64(7), want: false},
//		{op: expression.GreaterThanOperator, l: int64(6), r: bool(false), noFunc: true},
//		{op: expression.GreaterThanOperator, l: uint64(6), r: int64(7), want: false},
//		{op: expression.GreaterThanOperator, l: uint64(6), r: uint64(7), want: false},
//		{op: expression.GreaterThanOperator, l: uint64(6), r: float64(7), want: false},
//		{op: expression.GreaterThanOperator, l: uint64(6), r: bool(false), noFunc: true},
//		{op: expression.GreaterThanOperator, l: float64(6), r: int64(7), want: false},
//		{op: expression.GreaterThanOperator, l: float64(6), r: uint64(7), want: false},
//		{op: expression.GreaterThanOperator, l: float64(6), r: float64(7), want: false},
//		{op: expression.GreaterThanOperator, l: float64(6), r: bool(false), noFunc: true},
//		{op: expression.GreaterThanOperator, l: bool(true), r: int64(7), noFunc: true},
//		{op: expression.GreaterThanOperator, l: bool(true), r: uint64(7), noFunc: true},
//		{op: expression.GreaterThanOperator, l: bool(true), r: float64(7), noFunc: true},
//		{op: expression.GreaterThanOperator, l: bool(true), r: bool(false), noFunc: true},
//		{op: expression.EqualOperator, l: int64(6), r: int64(7), want: false},
//		{op: expression.EqualOperator, l: int64(6), r: uint64(7), want: false},
//		{op: expression.EqualOperator, l: int64(6), r: float64(7), want: false},
//		{op: expression.EqualOperator, l: int64(6), r: bool(false), noFunc: true},
//		{op: expression.EqualOperator, l: uint64(6), r: int64(7), want: false},
//		{op: expression.EqualOperator, l: uint64(6), r: uint64(7), want: false},
//		{op: expression.EqualOperator, l: uint64(6), r: float64(7), want: false},
//		{op: expression.EqualOperator, l: uint64(6), r: bool(false), noFunc: true},
//		{op: expression.EqualOperator, l: float64(6), r: int64(7), want: false},
//		{op: expression.EqualOperator, l: float64(6), r: uint64(7), want: false},
//		{op: expression.EqualOperator, l: float64(6), r: float64(7), want: false},
//		{op: expression.EqualOperator, l: float64(6), r: bool(false), noFunc: true},
//		{op: expression.EqualOperator, l: bool(true), r: int64(7), noFunc: true},
//		{op: expression.EqualOperator, l: bool(true), r: uint64(7), noFunc: true},
//		{op: expression.EqualOperator, l: bool(true), r: float64(7), noFunc: true},
//		{op: expression.EqualOperator, l: bool(true), r: bool(false), noFunc: true},
//		{op: expression.NotEqualOperator, l: int64(6), r: int64(7), want: true},
//		{op: expression.NotEqualOperator, l: int64(6), r: uint64(7), want: true},
//		{op: expression.NotEqualOperator, l: int64(6), r: float64(7), want: true},
//		{op: expression.NotEqualOperator, l: int64(6), r: bool(true), noFunc: true},
//		{op: expression.NotEqualOperator, l: uint64(6), r: int64(7), want: true},
//		{op: expression.NotEqualOperator, l: uint64(6), r: uint64(7), want: true},
//		{op: expression.NotEqualOperator, l: uint64(6), r: float64(7), want: true},
//		{op: expression.NotEqualOperator, l: uint64(6), r: bool(true), noFunc: true},
//		{op: expression.NotEqualOperator, l: float64(6), r: int64(7), want: true},
//		{op: expression.NotEqualOperator, l: float64(6), r: uint64(7), want: true},
//		{op: expression.NotEqualOperator, l: float64(6), r: float64(7), want: true},
//		{op: expression.NotEqualOperator, l: float64(6), r: bool(false), noFunc: true},
//		{op: expression.NotEqualOperator, l: bool(true), r: int64(7), noFunc: true},
//		{op: expression.NotEqualOperator, l: bool(true), r: uint64(7), noFunc: true},
//		{op: expression.NotEqualOperator, l: bool(true), r: float64(7), noFunc: true},
//		{op: expression.NotEqualOperator, l: bool(true), r: bool(false), noFunc: true},
//		{op: expression.AndOperator, l: int64(6), r: int64(7), noFunc: true},
//		{op: expression.AndOperator, l: int64(6), r: uint64(7), noFunc: true},
//		{op: expression.AndOperator, l: int64(6), r: float64(7), noFunc: true},
//		{op: expression.AndOperator, l: int64(6), r: bool(false), noFunc: true},
//		{op: expression.AndOperator, l: uint64(6), r: int64(7), noFunc: true},
//		{op: expression.AndOperator, l: uint64(6), r: uint64(7), noFunc: true},
//		{op: expression.AndOperator, l: uint64(6), r: float64(7), noFunc: true},
//		{op: expression.AndOperator, l: uint64(6), r: bool(false), noFunc: true},
//		{op: expression.AndOperator, l: float64(6), r: int64(7), noFunc: true},
//		{op: expression.AndOperator, l: float64(6), r: uint64(7), noFunc: true},
//		{op: expression.AndOperator, l: float64(6), r: float64(7), noFunc: true},
//		{op: expression.AndOperator, l: float64(6), r: bool(false), noFunc: true},
//		{op: expression.AndOperator, l: bool(true), r: int64(7), noFunc: true},
//		{op: expression.AndOperator, l: bool(true), r: uint64(7), noFunc: true},
//		{op: expression.AndOperator, l: bool(true), r: float64(7), noFunc: true},
//		{op: expression.AndOperator, l: bool(true), r: bool(false), want: false},
//		{op: expression.OrOperator, l: int64(6), r: int64(7), noFunc: true},
//		{op: expression.OrOperator, l: int64(6), r: uint64(7), noFunc: true},
//		{op: expression.OrOperator, l: int64(6), r: float64(7), noFunc: true},
//		{op: expression.OrOperator, l: int64(6), r: bool(false), noFunc: true},
//		{op: expression.OrOperator, l: uint64(6), r: int64(7), noFunc: true},
//		{op: expression.OrOperator, l: uint64(6), r: uint64(7), noFunc: true},
//		{op: expression.OrOperator, l: uint64(6), r: float64(7), noFunc: true},
//		{op: expression.OrOperator, l: uint64(6), r: bool(false), noFunc: true},
//		{op: expression.OrOperator, l: float64(6), r: int64(7), noFunc: true},
//		{op: expression.OrOperator, l: float64(6), r: uint64(7), noFunc: true},
//		{op: expression.OrOperator, l: float64(6), r: float64(7), noFunc: true},
//		{op: expression.OrOperator, l: float64(6), r: bool(false), noFunc: true},
//		{op: expression.OrOperator, l: bool(true), r: int64(7), noFunc: true},
//		{op: expression.OrOperator, l: bool(true), r: uint64(7), noFunc: true},
//		{op: expression.OrOperator, l: bool(true), r: float64(7), noFunc: true},
//		{op: expression.OrOperator, l: bool(true), r: bool(false), want: true},
//	}
//	for i, tc := range testCases {
//		tc := tc
//		t.Run(fmt.Sprintf("%d: %v %v %v", i, tc.l, tc.op, tc.r), func(t *testing.T) {
//			lt := typeOf(tc.l)
//			rt := typeOf(tc.r)
//			sig := binarySignature{
//				Operator: tc.op,
//				Left:     lt,
//				Right:    rt,
//			}
//			f, ok := binaryFuncs[sig]
//			if !ok {
//				if !tc.noFunc {
//					t.Fatal("could not find matching function")
//				}
//				return
//			} else if tc.noFunc {
//				t.Fatal("expected to not find function")
//			}
//			left := evaluator{
//				Value: tc.l,
//			}
//			right := evaluator{
//				Value: tc.r,
//			}
//
//			got := f.Func(nil, left, right)
//			want := Value{
//				Type:  typeOf(tc.want),
//				Value: tc.want,
//			}
//
//			if !cmp.Equal(got, want) {
//				t.Errorf("unexpected value: -want/+got\n%s", cmp.Diff(want, got))
//			}
//		})
//	}
//}
//
//func typeOf(v interface{}) DataType {
//	switch v.(type) {
//	case bool:
//		return TBool
//	case int64:
//		return TInt
//	case uint64:
//		return TUInt
//	case float64:
//		return TFloat
//	case string:
//		return TString
//	case Time:
//		return TTime
//	default:
//		return TInvalid
//	}
//}
//
//type evaluator struct {
//	Value interface{}
//}
//
//func (v evaluator) Type() DataType {
//	return typeOf(v.Value)
//}
//func (v evaluator) EvalBool(Scope) bool {
//	return v.Value.(bool)
//}
//func (v evaluator) EvalInt(Scope) int64 {
//	return v.Value.(int64)
//}
//func (v evaluator) EvalUInt(Scope) uint64 {
//	return v.Value.(uint64)
//}
//func (v evaluator) EvalFloat(Scope) float64 {
//	return v.Value.(float64)
//}
//func (v evaluator) EvalString(Scope) string {
//	return v.Value.(string)
//}
//func (v evaluator) EvalTime(Scope) Time {
//	return v.Value.(Time)
//}
