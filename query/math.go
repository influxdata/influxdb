package query

import (
	"time"

	"github.com/influxdata/influxql"
)

func isMathFunction(call *influxql.Call) bool {
	return false
}

type MathTypeMapper struct{}

func (MathTypeMapper) MapType(measurement *influxql.Measurement, field string) influxql.DataType {
	return influxql.Unknown
}

func (MathTypeMapper) CallType(name string, args []influxql.DataType) (influxql.DataType, error) {
	// TODO(jsternberg): Put math function call types here.
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
	if v, ok := v.Valuer.(influxql.CallValuer); ok {
		return v.Call(name, args)
	}
	return nil, false
}

func (v *MathValuer) Zone() *time.Location {
	if v, ok := v.Valuer.(influxql.ZoneValuer); ok {
		return v.Zone()
	}
	return nil
}
