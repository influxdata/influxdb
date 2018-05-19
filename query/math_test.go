package query_test

import (
	"math"
	"testing"

	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxql"
)

func TestMath_TypeMapper(t *testing.T) {
	for _, tt := range []struct {
		s   string
		typ influxql.DataType
		err bool
	}{
		{s: `abs(f::float)`, typ: influxql.Float},
		{s: `abs(i::integer)`, typ: influxql.Integer},
		{s: `abs(u::unsigned)`, typ: influxql.Unsigned},
		{s: `abs(s::string)`, err: true},
		{s: `abs(b::boolean)`, err: true},
		{s: `sin(f::float)`, typ: influxql.Float},
		{s: `sin(i::integer)`, typ: influxql.Float},
		{s: `sin(u::unsigned)`, typ: influxql.Float},
		{s: `sin(s::string)`, err: true},
		{s: `sin(b::boolean)`, err: true},
		{s: `cos(f::float)`, typ: influxql.Float},
		{s: `cos(i::integer)`, typ: influxql.Float},
		{s: `cos(u::unsigned)`, typ: influxql.Float},
		{s: `cos(s::string)`, err: true},
		{s: `cos(b::boolean)`, err: true},
		{s: `tan(f::float)`, typ: influxql.Float},
		{s: `tan(i::integer)`, typ: influxql.Float},
		{s: `tan(u::unsigned)`, typ: influxql.Float},
		{s: `tan(s::string)`, err: true},
		{s: `tan(b::boolean)`, err: true},
		{s: `asin(f::float)`, typ: influxql.Float},
		{s: `asin(i::integer)`, err: true},
		{s: `asin(u::unsigned)`, err: true},
		{s: `asin(s::string)`, err: true},
		{s: `asin(b::boolean)`, err: true},
		{s: `acos(f::float)`, typ: influxql.Float},
		{s: `acos(i::integer)`, err: true},
		{s: `acos(u::unsigned)`, err: true},
		{s: `acos(s::string)`, err: true},
		{s: `acos(b::boolean)`, err: true},
		{s: `atan(f::float)`, typ: influxql.Float},
		{s: `atan(i::integer)`, typ: influxql.Float},
		{s: `atan(u::unsigned)`, typ: influxql.Float},
		{s: `atan(s::string)`, err: true},
		{s: `atan(b::boolean)`, err: true},
		{s: `atan2(y::float, x::float)`, typ: influxql.Float},
		{s: `atan2(y::integer, x::float)`, typ: influxql.Float},
		{s: `atan2(y::unsigned, x::float)`, typ: influxql.Float},
		{s: `atan2(y::string, x::float)`, err: true},
		{s: `atan2(y::boolean, x::float)`, err: true},
		{s: `atan2(y::float, x::float)`, typ: influxql.Float},
		{s: `atan2(y::float, x::integer)`, typ: influxql.Float},
		{s: `atan2(y::float, x::unsigned)`, typ: influxql.Float},
		{s: `atan2(y::float, x::string)`, err: true},
		{s: `atan2(y::float, x::boolean)`, err: true},
		{s: `exp(f::float)`, typ: influxql.Float},
		{s: `exp(i::integer)`, typ: influxql.Float},
		{s: `exp(u::unsigned)`, typ: influxql.Float},
		{s: `exp(s::string)`, err: true},
		{s: `exp(b::boolean)`, err: true},
		{s: `log(f::float)`, typ: influxql.Float},
		{s: `log(i::integer)`, typ: influxql.Float},
		{s: `log(u::unsigned)`, typ: influxql.Float},
		{s: `log(s::string)`, err: true},
		{s: `log(b::boolean)`, err: true},
		{s: `ln(f::float)`, typ: influxql.Float},
		{s: `ln(i::integer)`, typ: influxql.Float},
		{s: `ln(u::unsigned)`, typ: influxql.Float},
		{s: `ln(s::string)`, err: true},
		{s: `ln(b::boolean)`, err: true},
		{s: `log2(f::float)`, typ: influxql.Float},
		{s: `log2(i::integer)`, typ: influxql.Float},
		{s: `log2(u::unsigned)`, typ: influxql.Float},
		{s: `log2(s::string)`, err: true},
		{s: `log2(b::boolean)`, err: true},
		{s: `log10(f::float)`, typ: influxql.Float},
		{s: `log10(i::integer)`, typ: influxql.Float},
		{s: `log10(u::unsigned)`, typ: influxql.Float},
		{s: `log10(s::string)`, err: true},
		{s: `log10(b::boolean)`, err: true},
		{s: `sqrt(f::float)`, typ: influxql.Float},
		{s: `sqrt(i::integer)`, typ: influxql.Float},
		{s: `sqrt(u::unsigned)`, typ: influxql.Float},
		{s: `sqrt(s::string)`, err: true},
		{s: `sqrt(b::boolean)`, err: true},
		{s: `pow(y::float, x::float)`, typ: influxql.Float},
		{s: `pow(y::integer, x::float)`, typ: influxql.Float},
		{s: `pow(y::unsigned, x::float)`, typ: influxql.Float},
		{s: `pow(y::string, x::string)`, err: true},
		{s: `pow(y::boolean, x::boolean)`, err: true},
		{s: `pow(y::float, x::float)`, typ: influxql.Float},
		{s: `pow(y::float, x::integer)`, typ: influxql.Float},
		{s: `pow(y::float, x::unsigned)`, typ: influxql.Float},
		{s: `pow(y::float, x::string)`, err: true},
		{s: `pow(y::float, x::boolean)`, err: true},
		{s: `floor(f::float)`, typ: influxql.Float},
		{s: `floor(i::integer)`, typ: influxql.Integer},
		{s: `floor(u::unsigned)`, typ: influxql.Unsigned},
		{s: `floor(s::string)`, err: true},
		{s: `floor(b::boolean)`, err: true},
		{s: `ceil(f::float)`, typ: influxql.Float},
		{s: `ceil(i::integer)`, typ: influxql.Integer},
		{s: `ceil(u::unsigned)`, typ: influxql.Unsigned},
		{s: `ceil(s::string)`, err: true},
		{s: `ceil(b::boolean)`, err: true},
		{s: `round(f::float)`, typ: influxql.Float},
		{s: `round(i::integer)`, typ: influxql.Integer},
		{s: `round(u::unsigned)`, typ: influxql.Unsigned},
		{s: `round(s::string)`, err: true},
		{s: `round(b::boolean)`, err: true},
	} {
		t.Run(tt.s, func(t *testing.T) {
			expr := MustParseExpr(tt.s)

			typmap := influxql.TypeValuerEval{
				TypeMapper: query.MathTypeMapper{},
			}
			if got, err := typmap.EvalType(expr); err != nil {
				if !tt.err {
					t.Errorf("unexpected error: %s", err)
				}
			} else if tt.err {
				t.Error("expected error")
			} else if want := tt.typ; got != want {
				t.Errorf("unexpected type:\n\t-: \"%s\"\n\t+: \"%s\"", want, got)
			}
		})
	}
}

func TestMathValuer_Call(t *testing.T) {
	type values map[string]interface{}
	for _, tt := range []struct {
		s      string
		values values
		exp    interface{}
	}{
		{s: `abs(f)`, values: values{"f": float64(2)}, exp: float64(2)},
		{s: `abs(f)`, values: values{"f": float64(-2)}, exp: float64(2)},
		{s: `abs(i)`, values: values{"i": int64(2)}, exp: int64(2)},
		{s: `abs(i)`, values: values{"i": int64(-2)}, exp: int64(-2)},
		{s: `abs(u)`, values: values{"u": uint64(2)}, exp: uint64(2)},
		{s: `sin(f)`, values: values{"f": math.Pi / 2}, exp: math.Sin(math.Pi / 2)},
		{s: `sin(i)`, values: values{"i": int64(2)}, exp: math.Sin(2)},
		{s: `sin(u)`, values: values{"u": uint64(2)}, exp: math.Sin(2)},
		{s: `asin(f)`, values: values{"f": float64(0.5)}, exp: math.Asin(0.5)},
		{s: `cos(f)`, values: values{"f": math.Pi / 2}, exp: math.Cos(math.Pi / 2)},
		{s: `cos(i)`, values: values{"i": int64(2)}, exp: math.Cos(2)},
		{s: `cos(u)`, values: values{"u": uint64(2)}, exp: math.Cos(2)},
		{s: `acos(f)`, values: values{"f": float64(0.5)}, exp: math.Acos(0.5)},
		{s: `tan(f)`, values: values{"f": math.Pi / 2}, exp: math.Tan(math.Pi / 2)},
		{s: `tan(i)`, values: values{"i": int64(2)}, exp: math.Tan(2)},
		{s: `tan(u)`, values: values{"u": uint64(2)}, exp: math.Tan(2)},
		{s: `atan(f)`, values: values{"f": float64(2)}, exp: math.Atan(2)},
		{s: `atan(i)`, values: values{"i": int64(2)}, exp: math.Atan(2)},
		{s: `atan(u)`, values: values{"u": uint64(2)}, exp: math.Atan(2)},
		{s: `atan2(y, x)`, values: values{"y": float64(2), "x": float64(3)}, exp: math.Atan2(2, 3)},
		{s: `atan2(y, x)`, values: values{"y": int64(2), "x": int64(3)}, exp: math.Atan2(2, 3)},
		{s: `atan2(y, x)`, values: values{"y": uint64(2), "x": uint64(3)}, exp: math.Atan2(2, 3)},
		{s: `floor(f)`, values: values{"f": float64(2.5)}, exp: float64(2)},
		{s: `floor(i)`, values: values{"i": int64(2)}, exp: int64(2)},
		{s: `floor(u)`, values: values{"u": uint64(2)}, exp: uint64(2)},
		{s: `ceil(f)`, values: values{"f": float64(2.5)}, exp: float64(3)},
		{s: `ceil(i)`, values: values{"i": int64(2)}, exp: int64(2)},
		{s: `ceil(u)`, values: values{"u": uint64(2)}, exp: uint64(2)},
		{s: `round(f)`, values: values{"f": float64(2.4)}, exp: float64(2)},
		{s: `round(f)`, values: values{"f": float64(2.6)}, exp: float64(3)},
		{s: `round(i)`, values: values{"i": int64(2)}, exp: int64(2)},
		{s: `round(u)`, values: values{"u": uint64(2)}, exp: uint64(2)},
		{s: `exp(f)`, values: values{"f": float64(3)}, exp: math.Exp(3)},
		{s: `exp(i)`, values: values{"i": int64(3)}, exp: math.Exp(3)},
		{s: `exp(u)`, values: values{"u": uint64(3)}, exp: math.Exp(3)},
		{s: `log(f, 8)`, values: values{"f": float64(3)}, exp: math.Log(3) / math.Log(8)},
		{s: `log(i, 8)`, values: values{"i": int64(3)}, exp: math.Log(3) / math.Log(8)},
		{s: `log(u, 8)`, values: values{"u": uint64(3)}, exp: math.Log(3) / math.Log(8)},
		{s: `ln(f)`, values: values{"f": float64(3)}, exp: math.Log(3)},
		{s: `ln(i)`, values: values{"i": int64(3)}, exp: math.Log(3)},
		{s: `ln(u)`, values: values{"u": uint64(3)}, exp: math.Log(3)},
		{s: `log2(f)`, values: values{"f": float64(3)}, exp: math.Log2(3)},
		{s: `log2(i)`, values: values{"i": int64(3)}, exp: math.Log2(3)},
		{s: `log2(u)`, values: values{"u": uint64(3)}, exp: math.Log2(3)},
		{s: `log10(f)`, values: values{"f": float64(3)}, exp: math.Log10(3)},
		{s: `log10(i)`, values: values{"i": int64(3)}, exp: math.Log10(3)},
		{s: `log10(u)`, values: values{"u": uint64(3)}, exp: math.Log10(3)},
		{s: `sqrt(f)`, values: values{"f": float64(3)}, exp: math.Sqrt(3)},
		{s: `sqrt(i)`, values: values{"i": int64(3)}, exp: math.Sqrt(3)},
		{s: `sqrt(u)`, values: values{"u": uint64(3)}, exp: math.Sqrt(3)},
		{s: `pow(f, 2)`, values: values{"f": float64(4)}, exp: math.Pow(4, 2)},
		{s: `pow(i, 2)`, values: values{"i": int64(4)}, exp: math.Pow(4, 2)},
		{s: `pow(u, 2)`, values: values{"u": uint64(4)}, exp: math.Pow(4, 2)},
	} {
		t.Run(tt.s, func(t *testing.T) {
			expr := MustParseExpr(tt.s)

			valuer := influxql.ValuerEval{
				Valuer: influxql.MultiValuer(
					influxql.MapValuer(tt.values),
					query.MathValuer{},
				),
			}
			if got, want := valuer.Eval(expr), tt.exp; got != want {
				t.Errorf("unexpected value: %v != %v", want, got)
			}
		})
	}
}
