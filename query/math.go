package query

import (
	"math"
	"time"

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

type MathValuer struct {
	Valuer influxql.Valuer
}

func (v *MathValuer) Value(key string) (interface{}, bool) {
	if v.Valuer != nil {
		return v.Valuer.Value(key)
	}
	return nil, false
}

func (v *MathValuer) Call(name string, args []influxql.Expr) (interface{}, bool) {
	if len(args) == 1 {
		switch name {
		case "sin":
			return v.callTrigFunction(math.Sin, args[0])
		case "cos":
			return v.callTrigFunction(math.Cos, args[0])
		case "tan":
			return v.callTrigFunction(math.Tan, args[0])
		}
	}
	if v, ok := v.Valuer.(influxql.CallValuer); ok {
		return v.Call(name, args)
	}
	return nil, false
}

func (v *MathValuer) callTrigFunction(fn func(x float64) float64, arg0 influxql.Expr) (interface{}, bool) {
	var value float64
	switch arg0 := arg0.(type) {
	case *influxql.NumberLiteral:
		value = arg0.Val
	case *influxql.IntegerLiteral:
		value = float64(arg0.Val)
	case *influxql.VarRef:
		if v.Valuer == nil {
			return nil, false
		} else if val, ok := v.Valuer.Value(arg0.Val); ok {
			switch val := val.(type) {
			case float64:
				value = val
			case int64:
				value = float64(val)
			}
		} else {
			return nil, false
		}
	default:
		return nil, false
	}
	return fn(value), true
}

func (v *MathValuer) Zone() *time.Location {
	if v, ok := v.Valuer.(influxql.ZoneValuer); ok {
		return v.Zone()
	}
	return nil
}
