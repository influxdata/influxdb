package query

import (
	"math"

	"github.com/influxdata/influxql"
)

func isMathFunction(call *influxql.Call) bool {
	switch call.Name {
	case "sin", "cos", "tan":
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
