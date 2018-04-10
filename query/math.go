package query

import (
	"fmt"
	"math"

	"github.com/influxdata/influxql"
)

func isMathFunction(call *influxql.Call) bool {
	switch call.Name {
	case "sin", "cos", "tan", "floor", "ceil", "round":
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
	case "sin", "cos", "tan":
		return influxql.Float, nil
	case "floor", "ceil", "round":
		switch args[0] {
		case influxql.Float, influxql.Integer, influxql.Unsigned, influxql.Unknown:
			return args[0], nil
		default:
			return influxql.Unknown, fmt.Errorf("invalid argument type for first argument in %s(): %s", name, args[0])
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
		case "sin":
			return v.callTrigFunction(math.Sin, arg0)
		case "cos":
			return v.callTrigFunction(math.Cos, arg0)
		case "tan":
			return v.callTrigFunction(math.Tan, arg0)
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
		}
	}
	return nil, false
}

func (MathValuer) callTrigFunction(fn func(x float64) float64, arg0 interface{}) (interface{}, bool) {
	var value float64
	switch arg0 := arg0.(type) {
	case float64:
		value = arg0
	case int64:
		value = float64(arg0)
	default:
		return nil, false
	}
	return fn(value), true
}

func round(x float64) float64 {
	t := math.Trunc(x)
	if math.Abs(x-t) >= 0.5 {
		return t + math.Copysign(1, x)
	}
	return t
}
