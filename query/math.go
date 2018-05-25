package query

import (
	"fmt"
	"math"

	"github.com/influxdata/influxql"
)

func isMathFunction(call *influxql.Call) bool {
	switch call.Name {
	case "abs", "sin", "cos", "tan", "asin", "acos", "atan", "atan2", "exp", "log", "ln", "log2", "log10", "sqrt", "pow", "floor", "ceil", "round":
		return true
	}
	return false
}

type MathTypeMapper struct{}

func (MathTypeMapper) MapType(measurement *influxql.Measurement, field string) influxql.DataType {
	return influxql.Unknown
}

func (MathTypeMapper) CallType(name string, args []influxql.DataType) (influxql.DataType, error) {
	switch name {
	case "sin", "cos", "tan", "atan", "exp", "log", "ln", "log2", "log10", "sqrt":
		var arg0 influxql.DataType
		if len(args) > 0 {
			arg0 = args[0]
		}
		switch arg0 {
		case influxql.Float, influxql.Integer, influxql.Unsigned, influxql.Unknown:
			return influxql.Float, nil
		default:
			return influxql.Unknown, fmt.Errorf("invalid argument type for the first argument in %s(): %s", name, arg0)
		}
	case "asin", "acos":
		var arg0 influxql.DataType
		if len(args) > 0 {
			arg0 = args[0]
		}
		switch arg0 {
		case influxql.Float, influxql.Unknown:
			return influxql.Float, nil
		default:
			return influxql.Unknown, fmt.Errorf("invalid argument type for the first argument in %s(): %s", name, arg0)
		}
	case "atan2", "pow":
		var arg0, arg1 influxql.DataType
		if len(args) > 0 {
			arg0 = args[0]
		}
		if len(args) > 1 {
			arg1 = args[1]
		}

		switch arg0 {
		case influxql.Float, influxql.Integer, influxql.Unsigned, influxql.Unknown:
			// Pass through to verify the second argument.
		default:
			return influxql.Unknown, fmt.Errorf("invalid argument type for the first argument in %s(): %s", name, arg0)
		}

		switch arg1 {
		case influxql.Float, influxql.Integer, influxql.Unsigned, influxql.Unknown:
			return influxql.Float, nil
		default:
			return influxql.Unknown, fmt.Errorf("invalid argument type for the second argument in %s(): %s", name, arg1)
		}
	case "abs", "floor", "ceil", "round":
		var arg0 influxql.DataType
		if len(args) > 0 {
			arg0 = args[0]
		}
		switch arg0 {
		case influxql.Float, influxql.Integer, influxql.Unsigned, influxql.Unknown:
			return args[0], nil
		default:
			return influxql.Unknown, fmt.Errorf("invalid argument type for the first argument in %s(): %s", name, arg0)
		}
	}
	return influxql.Unknown, nil
}

type MathValuer struct{}

var _ influxql.CallValuer = MathValuer{}

func (MathValuer) Value(key string) (interface{}, bool) {
	return nil, false
}

func (v MathValuer) Call(name string, args []interface{}) (interface{}, bool) {
	if len(args) == 1 {
		arg0 := args[0]
		switch name {
		case "abs":
			switch arg0 := arg0.(type) {
			case float64:
				return math.Abs(arg0), true
			case int64, uint64:
				return arg0, true
			default:
				return nil, true
			}
		case "sin":
			if arg0, ok := asFloat(arg0); ok {
				return math.Sin(arg0), true
			}
			return nil, true
		case "cos":
			if arg0, ok := asFloat(arg0); ok {
				return math.Cos(arg0), true
			}
			return nil, true
		case "tan":
			if arg0, ok := asFloat(arg0); ok {
				return math.Tan(arg0), true
			}
			return nil, true
		case "floor":
			switch arg0 := arg0.(type) {
			case float64:
				return math.Floor(arg0), true
			case int64, uint64:
				return arg0, true
			default:
				return nil, true
			}
		case "ceil":
			switch arg0 := arg0.(type) {
			case float64:
				return math.Ceil(arg0), true
			case int64, uint64:
				return arg0, true
			default:
				return nil, true
			}
		case "round":
			switch arg0 := arg0.(type) {
			case float64:
				return round(arg0), true
			case int64, uint64:
				return arg0, true
			default:
				return nil, true
			}
		case "asin":
			if arg0, ok := asFloat(arg0); ok {
				return math.Asin(arg0), true
			}
			return nil, true
		case "acos":
			if arg0, ok := asFloat(arg0); ok {
				return math.Acos(arg0), true
			}
			return nil, true
		case "atan":
			if arg0, ok := asFloat(arg0); ok {
				return math.Atan(arg0), true
			}
			return nil, true
		case "exp":
			if arg0, ok := asFloat(arg0); ok {
				return math.Exp(arg0), true
			}
			return nil, true
		case "ln":
			if arg0, ok := asFloat(arg0); ok {
				return math.Log(arg0), true
			}
			return nil, true
		case "log2":
			if arg0, ok := asFloat(arg0); ok {
				return math.Log2(arg0), true
			}
			return nil, true
		case "log10":
			if arg0, ok := asFloat(arg0); ok {
				return math.Log10(arg0), true
			}
			return nil, true
		case "sqrt":
			if arg0, ok := asFloat(arg0); ok {
				return math.Sqrt(arg0), true
			}
			return nil, true
		}
	} else if len(args) == 2 {
		arg0, arg1 := args[0], args[1]
		switch name {
		case "atan2":
			if arg0, arg1, ok := asFloats(arg0, arg1); ok {
				return math.Atan2(arg0, arg1), true
			}
			return nil, true
		case "log":
			if arg0, arg1, ok := asFloats(arg0, arg1); ok {
				return math.Log(arg0) / math.Log(arg1), true
			}
			return nil, true
		case "pow":
			if arg0, arg1, ok := asFloats(arg0, arg1); ok {
				return math.Pow(arg0, arg1), true
			}
			return nil, true
		}
	}
	return nil, false
}

func asFloat(x interface{}) (float64, bool) {
	switch arg0 := x.(type) {
	case float64:
		return arg0, true
	case int64:
		return float64(arg0), true
	case uint64:
		return float64(arg0), true
	default:
		return 0, false
	}
}

func asFloats(x, y interface{}) (float64, float64, bool) {
	arg0, ok := asFloat(x)
	if !ok {
		return 0, 0, false
	}
	arg1, ok := asFloat(y)
	if !ok {
		return 0, 0, false
	}
	return arg0, arg1, true
}

func round(x float64) float64 {
	t := math.Trunc(x)
	if math.Abs(x-t) >= 0.5 {
		return t + math.Copysign(1, x)
	}
	return t
}
