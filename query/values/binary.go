package values

import (
	"fmt"

	"github.com/influxdata/platform/query/ast"
	"github.com/influxdata/platform/query/semantic"
)

type BinaryFunction func(l, r Value) Value

type BinaryFuncSignature struct {
	Operator    ast.OperatorKind
	Left, Right semantic.Type
}

func LookupBinaryFunction(sig BinaryFuncSignature) (BinaryFunction, error) {
	f, ok := binaryFuncLookup[sig]
	if !ok {
		return nil, fmt.Errorf("unsupported binary expression %v %v %v", sig.Left, sig.Operator, sig.Right)
	}
	return f, nil
}

var binaryFuncLookup = map[BinaryFuncSignature]BinaryFunction{
	//---------------
	// Math Operators
	//---------------
	{Operator: ast.AdditionOperator, Left: semantic.Int, Right: semantic.Int}: func(lv, rv Value) Value {
		l := lv.Int()
		r := rv.Int()
		return NewIntValue(l + r)
	},
	{Operator: ast.AdditionOperator, Left: semantic.UInt, Right: semantic.UInt}: func(lv, rv Value) Value {
		l := lv.UInt()
		r := rv.UInt()
		return NewUIntValue(l + r)
	},
	{Operator: ast.AdditionOperator, Left: semantic.Float, Right: semantic.Float}: func(lv, rv Value) Value {
		l := lv.Float()
		r := rv.Float()
		return NewFloatValue(l + r)
	},
	{Operator: ast.SubtractionOperator, Left: semantic.Int, Right: semantic.Int}: func(lv, rv Value) Value {
		l := lv.Int()
		r := rv.Int()
		return NewIntValue(l - r)
	},
	{Operator: ast.SubtractionOperator, Left: semantic.UInt, Right: semantic.UInt}: func(lv, rv Value) Value {
		l := lv.UInt()
		r := rv.UInt()
		return NewUIntValue(l - r)
	},
	{Operator: ast.SubtractionOperator, Left: semantic.Float, Right: semantic.Float}: func(lv, rv Value) Value {
		l := lv.Float()
		r := rv.Float()
		return NewFloatValue(l - r)
	},
	{Operator: ast.MultiplicationOperator, Left: semantic.Int, Right: semantic.Int}: func(lv, rv Value) Value {
		l := lv.Int()
		r := rv.Int()
		return NewIntValue(l * r)
	},
	{Operator: ast.MultiplicationOperator, Left: semantic.UInt, Right: semantic.UInt}: func(lv, rv Value) Value {
		l := lv.UInt()
		r := rv.UInt()
		return NewUIntValue(l * r)
	},
	{Operator: ast.MultiplicationOperator, Left: semantic.Float, Right: semantic.Float}: func(lv, rv Value) Value {
		l := lv.Float()
		r := rv.Float()
		return NewFloatValue(l * r)
	},
	{Operator: ast.DivisionOperator, Left: semantic.Int, Right: semantic.Int}: func(lv, rv Value) Value {
		l := lv.Int()
		r := rv.Int()
		return NewIntValue(l / r)
	},
	{Operator: ast.DivisionOperator, Left: semantic.UInt, Right: semantic.UInt}: func(lv, rv Value) Value {
		l := lv.UInt()
		r := rv.UInt()
		return NewUIntValue(l / r)
	},
	{Operator: ast.DivisionOperator, Left: semantic.Float, Right: semantic.Float}: func(lv, rv Value) Value {
		l := lv.Float()
		r := rv.Float()
		return NewFloatValue(l / r)
	},

	//---------------------
	// Comparison Operators
	//---------------------

	// LessThanEqualOperator

	{Operator: ast.LessThanEqualOperator, Left: semantic.Int, Right: semantic.Int}: func(lv, rv Value) Value {
		l := lv.Int()
		r := rv.Int()
		return NewBoolValue(l <= r)
	},
	{Operator: ast.LessThanEqualOperator, Left: semantic.Int, Right: semantic.UInt}: func(lv, rv Value) Value {
		l := lv.Int()
		r := rv.UInt()
		if l < 0 {
			return NewBoolValue(true)
		}
		return NewBoolValue(uint64(l) <= r)
	},
	{Operator: ast.LessThanEqualOperator, Left: semantic.Int, Right: semantic.Float}: func(lv, rv Value) Value {
		l := lv.Int()
		r := rv.Float()
		return NewBoolValue(float64(l) <= r)
	},
	{Operator: ast.LessThanEqualOperator, Left: semantic.UInt, Right: semantic.Int}: func(lv, rv Value) Value {
		l := lv.UInt()
		r := rv.Int()
		if r < 0 {
			return NewBoolValue(false)
		}
		return NewBoolValue(l <= uint64(r))
	},
	{Operator: ast.LessThanEqualOperator, Left: semantic.UInt, Right: semantic.UInt}: func(lv, rv Value) Value {
		l := lv.UInt()
		r := rv.UInt()
		return NewBoolValue(l <= r)
	},
	{Operator: ast.LessThanEqualOperator, Left: semantic.UInt, Right: semantic.Float}: func(lv, rv Value) Value {
		l := lv.UInt()
		r := rv.Float()
		return NewBoolValue(float64(l) <= r)
	},
	{Operator: ast.LessThanEqualOperator, Left: semantic.Float, Right: semantic.Int}: func(lv, rv Value) Value {
		l := lv.Float()
		r := rv.Int()
		return NewBoolValue(l <= float64(r))
	},
	{Operator: ast.LessThanEqualOperator, Left: semantic.Float, Right: semantic.UInt}: func(lv, rv Value) Value {
		l := lv.Float()
		r := rv.UInt()
		return NewBoolValue(l <= float64(r))
	},
	{Operator: ast.LessThanEqualOperator, Left: semantic.Float, Right: semantic.Float}: func(lv, rv Value) Value {
		l := lv.Float()
		r := rv.Float()
		return NewBoolValue(l <= r)
	},

	// LessThanOperator

	{Operator: ast.LessThanOperator, Left: semantic.Int, Right: semantic.Int}: func(lv, rv Value) Value {
		l := lv.Int()
		r := rv.Int()
		return NewBoolValue(l < r)
	},
	{Operator: ast.LessThanOperator, Left: semantic.Int, Right: semantic.UInt}: func(lv, rv Value) Value {
		l := lv.Int()
		r := rv.UInt()
		if l < 0 {
			return NewBoolValue(true)
		}
		return NewBoolValue(uint64(l) < r)
	},
	{Operator: ast.LessThanOperator, Left: semantic.Int, Right: semantic.Float}: func(lv, rv Value) Value {
		l := lv.Int()
		r := rv.Float()
		return NewBoolValue(float64(l) < r)
	},
	{Operator: ast.LessThanOperator, Left: semantic.UInt, Right: semantic.Int}: func(lv, rv Value) Value {
		l := lv.UInt()
		r := rv.Int()
		if r < 0 {
			return NewBoolValue(false)
		}
		return NewBoolValue(l < uint64(r))
	},
	{Operator: ast.LessThanOperator, Left: semantic.UInt, Right: semantic.UInt}: func(lv, rv Value) Value {
		l := lv.UInt()
		r := rv.UInt()
		return NewBoolValue(l < r)
	},
	{Operator: ast.LessThanOperator, Left: semantic.UInt, Right: semantic.Float}: func(lv, rv Value) Value {
		l := lv.UInt()
		r := rv.Float()
		return NewBoolValue(float64(l) < r)
	},
	{Operator: ast.LessThanOperator, Left: semantic.Float, Right: semantic.Int}: func(lv, rv Value) Value {
		l := lv.Float()
		r := rv.Int()
		return NewBoolValue(l < float64(r))
	},
	{Operator: ast.LessThanOperator, Left: semantic.Float, Right: semantic.UInt}: func(lv, rv Value) Value {
		l := lv.Float()
		r := rv.UInt()
		return NewBoolValue(l < float64(r))
	},
	{Operator: ast.LessThanOperator, Left: semantic.Float, Right: semantic.Float}: func(lv, rv Value) Value {
		l := lv.Float()
		r := rv.Float()
		return NewBoolValue(l < r)
	},

	// GreaterThanEqualOperator

	{Operator: ast.GreaterThanEqualOperator, Left: semantic.Int, Right: semantic.Int}: func(lv, rv Value) Value {
		l := lv.Int()
		r := rv.Int()
		return NewBoolValue(l >= r)
	},
	{Operator: ast.GreaterThanEqualOperator, Left: semantic.Int, Right: semantic.UInt}: func(lv, rv Value) Value {
		l := lv.Int()
		r := rv.UInt()
		if l < 0 {
			return NewBoolValue(true)
		}
		return NewBoolValue(uint64(l) >= r)
	},
	{Operator: ast.GreaterThanEqualOperator, Left: semantic.Int, Right: semantic.Float}: func(lv, rv Value) Value {
		l := lv.Int()
		r := rv.Float()
		return NewBoolValue(float64(l) >= r)
	},
	{Operator: ast.GreaterThanEqualOperator, Left: semantic.UInt, Right: semantic.Int}: func(lv, rv Value) Value {
		l := lv.UInt()
		r := rv.Int()
		if r < 0 {
			return NewBoolValue(false)
		}
		return NewBoolValue(l >= uint64(r))
	},
	{Operator: ast.GreaterThanEqualOperator, Left: semantic.UInt, Right: semantic.UInt}: func(lv, rv Value) Value {
		l := lv.UInt()
		r := rv.UInt()
		return NewBoolValue(l >= r)
	},
	{Operator: ast.GreaterThanEqualOperator, Left: semantic.UInt, Right: semantic.Float}: func(lv, rv Value) Value {
		l := lv.UInt()
		r := rv.Float()
		return NewBoolValue(float64(l) >= r)
	},
	{Operator: ast.GreaterThanEqualOperator, Left: semantic.Float, Right: semantic.Int}: func(lv, rv Value) Value {
		l := lv.Float()
		r := rv.Int()
		return NewBoolValue(l >= float64(r))
	},
	{Operator: ast.GreaterThanEqualOperator, Left: semantic.Float, Right: semantic.UInt}: func(lv, rv Value) Value {
		l := lv.Float()
		r := rv.UInt()
		return NewBoolValue(l >= float64(r))
	},
	{Operator: ast.GreaterThanEqualOperator, Left: semantic.Float, Right: semantic.Float}: func(lv, rv Value) Value {
		l := lv.Float()
		r := rv.Float()
		return NewBoolValue(l >= r)
	},

	// GreaterThanOperator

	{Operator: ast.GreaterThanOperator, Left: semantic.Int, Right: semantic.Int}: func(lv, rv Value) Value {
		l := lv.Int()
		r := rv.Int()
		return NewBoolValue(l > r)
	},
	{Operator: ast.GreaterThanOperator, Left: semantic.Int, Right: semantic.UInt}: func(lv, rv Value) Value {
		l := lv.Int()
		r := rv.UInt()
		if l < 0 {
			return NewBoolValue(true)
		}
		return NewBoolValue(uint64(l) > r)
	},
	{Operator: ast.GreaterThanOperator, Left: semantic.Int, Right: semantic.Float}: func(lv, rv Value) Value {
		l := lv.Int()
		r := rv.Float()
		return NewBoolValue(float64(l) > r)
	},
	{Operator: ast.GreaterThanOperator, Left: semantic.UInt, Right: semantic.Int}: func(lv, rv Value) Value {
		l := lv.UInt()
		r := rv.Int()
		if r < 0 {
			return NewBoolValue(false)
		}
		return NewBoolValue(l > uint64(r))
	},
	{Operator: ast.GreaterThanOperator, Left: semantic.UInt, Right: semantic.UInt}: func(lv, rv Value) Value {
		l := lv.UInt()
		r := rv.UInt()
		return NewBoolValue(l > r)
	},
	{Operator: ast.GreaterThanOperator, Left: semantic.UInt, Right: semantic.Float}: func(lv, rv Value) Value {
		l := lv.UInt()
		r := rv.Float()
		return NewBoolValue(float64(l) > r)
	},
	{Operator: ast.GreaterThanOperator, Left: semantic.Float, Right: semantic.Int}: func(lv, rv Value) Value {
		l := lv.Float()
		r := rv.Int()
		return NewBoolValue(l > float64(r))
	},
	{Operator: ast.GreaterThanOperator, Left: semantic.Float, Right: semantic.UInt}: func(lv, rv Value) Value {
		l := lv.Float()
		r := rv.UInt()
		return NewBoolValue(l > float64(r))
	},
	{Operator: ast.GreaterThanOperator, Left: semantic.Float, Right: semantic.Float}: func(lv, rv Value) Value {
		l := lv.Float()
		r := rv.Float()
		return NewBoolValue(l > r)
	},

	// EqualOperator

	{Operator: ast.EqualOperator, Left: semantic.Int, Right: semantic.Int}: func(lv, rv Value) Value {
		l := lv.Int()
		r := rv.Int()
		return NewBoolValue(l == r)
	},
	{Operator: ast.EqualOperator, Left: semantic.Int, Right: semantic.UInt}: func(lv, rv Value) Value {
		l := lv.Int()
		r := rv.UInt()
		if l < 0 {
			return NewBoolValue(false)
		}
		return NewBoolValue(uint64(l) == r)
	},
	{Operator: ast.EqualOperator, Left: semantic.Int, Right: semantic.Float}: func(lv, rv Value) Value {
		l := lv.Int()
		r := rv.Float()
		return NewBoolValue(float64(l) == r)
	},
	{Operator: ast.EqualOperator, Left: semantic.UInt, Right: semantic.Int}: func(lv, rv Value) Value {
		l := lv.UInt()
		r := rv.Int()
		if r < 0 {
			return NewBoolValue(false)
		}
		return NewBoolValue(l == uint64(r))
	},
	{Operator: ast.EqualOperator, Left: semantic.UInt, Right: semantic.UInt}: func(lv, rv Value) Value {
		l := lv.UInt()
		r := rv.UInt()
		return NewBoolValue(l == r)
	},
	{Operator: ast.EqualOperator, Left: semantic.UInt, Right: semantic.Float}: func(lv, rv Value) Value {
		l := lv.UInt()
		r := rv.Float()
		return NewBoolValue(float64(l) == r)
	},
	{Operator: ast.EqualOperator, Left: semantic.Float, Right: semantic.Int}: func(lv, rv Value) Value {
		l := lv.Float()
		r := rv.Int()
		return NewBoolValue(l == float64(r))
	},
	{Operator: ast.EqualOperator, Left: semantic.Float, Right: semantic.UInt}: func(lv, rv Value) Value {
		l := lv.Float()
		r := rv.UInt()
		return NewBoolValue(l == float64(r))
	},
	{Operator: ast.EqualOperator, Left: semantic.Float, Right: semantic.Float}: func(lv, rv Value) Value {
		l := lv.Float()
		r := rv.Float()
		return NewBoolValue(l == r)
	},
	{Operator: ast.EqualOperator, Left: semantic.String, Right: semantic.String}: func(lv, rv Value) Value {
		l := lv.Str()
		r := rv.Str()
		return NewBoolValue(l == r)
	},

	// NotEqualOperator

	{Operator: ast.NotEqualOperator, Left: semantic.Int, Right: semantic.Int}: func(lv, rv Value) Value {
		l := lv.Int()
		r := rv.Int()
		return NewBoolValue(l != r)
	},
	{Operator: ast.NotEqualOperator, Left: semantic.Int, Right: semantic.UInt}: func(lv, rv Value) Value {
		l := lv.Int()
		r := rv.UInt()
		if l < 0 {
			return NewBoolValue(true)
		}
		return NewBoolValue(uint64(l) != r)
	},
	{Operator: ast.NotEqualOperator, Left: semantic.Int, Right: semantic.Float}: func(lv, rv Value) Value {
		l := lv.Int()
		r := rv.Float()
		return NewBoolValue(float64(l) != r)
	},
	{Operator: ast.NotEqualOperator, Left: semantic.UInt, Right: semantic.Int}: func(lv, rv Value) Value {
		l := lv.UInt()
		r := rv.Int()
		if r < 0 {
			return NewBoolValue(true)
		}
		return NewBoolValue(l != uint64(r))
	},
	{Operator: ast.NotEqualOperator, Left: semantic.UInt, Right: semantic.UInt}: func(lv, rv Value) Value {
		l := lv.UInt()
		r := rv.UInt()
		return NewBoolValue(l != r)
	},
	{Operator: ast.NotEqualOperator, Left: semantic.UInt, Right: semantic.Float}: func(lv, rv Value) Value {
		l := lv.UInt()
		r := rv.Float()
		return NewBoolValue(float64(l) != r)
	},
	{Operator: ast.NotEqualOperator, Left: semantic.Float, Right: semantic.Int}: func(lv, rv Value) Value {
		l := lv.Float()
		r := rv.Int()
		return NewBoolValue(l != float64(r))
	},
	{Operator: ast.NotEqualOperator, Left: semantic.Float, Right: semantic.UInt}: func(lv, rv Value) Value {
		l := lv.Float()
		r := rv.UInt()
		return NewBoolValue(l != float64(r))
	},
	{Operator: ast.NotEqualOperator, Left: semantic.Float, Right: semantic.Float}: func(lv, rv Value) Value {
		l := lv.Float()
		r := rv.Float()
		return NewBoolValue(l != r)
	},
	{Operator: ast.NotEqualOperator, Left: semantic.String, Right: semantic.String}: func(lv, rv Value) Value {
		l := lv.Str()
		r := rv.Str()
		return NewBoolValue(l != r)
	},
	{Operator: ast.RegexpMatchOperator, Left: semantic.String, Right: semantic.Regexp}: func(lv, rv Value) Value {
		l := lv.Str()
		r := rv.Regexp()
		return NewBoolValue(r.MatchString(l))
	},
	{Operator: ast.RegexpMatchOperator, Left: semantic.Regexp, Right: semantic.String}: func(lv, rv Value) Value {
		l := lv.Regexp()
		r := rv.Str()
		return NewBoolValue(l.MatchString(r))
	},
	{Operator: ast.NotRegexpMatchOperator, Left: semantic.String, Right: semantic.Regexp}: func(lv, rv Value) Value {
		l := lv.Str()
		r := rv.Regexp()
		return NewBoolValue(!r.MatchString(l))
	},
	{Operator: ast.NotRegexpMatchOperator, Left: semantic.Regexp, Right: semantic.String}: func(lv, rv Value) Value {
		l := lv.Regexp()
		r := rv.Str()
		return NewBoolValue(!l.MatchString(r))
	},

	{Operator: ast.AdditionOperator, Left: semantic.String, Right: semantic.String}: func(lv, rv Value) Value {
		l := lv.Str()
		r := rv.Str()
		return NewStringValue(l + r)
	},
}
