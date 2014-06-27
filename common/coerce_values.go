package common

import (
	"github.com/influxdb/influxdb/protocol"
)

type Type int

const (
	TYPE_INT = iota
	TYPE_STRING
	TYPE_BOOL
	TYPE_DOUBLE
	TYPE_UNKNOWN
)

func getValue(value *protocol.FieldValue) (interface{}, Type) {
	if value.Int64Value != nil {
		return value.Int64Value, TYPE_INT
	}
	if value.DoubleValue != nil {
		return value.DoubleValue, TYPE_DOUBLE
	}
	if value.BoolValue != nil {
		return value.BoolValue, TYPE_BOOL
	}
	if value.StringValue != nil {
		return value.StringValue, TYPE_STRING
	}

	return nil, TYPE_UNKNOWN
}

func CoerceValues(leftValue, rightValue *protocol.FieldValue) (interface{}, interface{}, Type) {
	if leftValue == nil {
		value, t := getValue(rightValue)
		return nil, value, t
	}

	if rightValue == nil {
		value, t := getValue(leftValue)
		return value, nil, t
	}

	if leftValue.Int64Value != nil {
		if rightValue.Int64Value != nil {
			return *leftValue.Int64Value, *rightValue.Int64Value, TYPE_INT
		} else if rightValue.DoubleValue != nil {
			return float64(*leftValue.Int64Value), *rightValue.DoubleValue, TYPE_DOUBLE
		}
		return nil, nil, TYPE_UNKNOWN
	}

	if leftValue.DoubleValue != nil {
		if rightValue.Int64Value != nil {
			return *leftValue.DoubleValue, float64(*rightValue.Int64Value), TYPE_DOUBLE
		} else if rightValue.DoubleValue != nil {
			return *leftValue.DoubleValue, *rightValue.DoubleValue, TYPE_DOUBLE
		}
		return nil, nil, TYPE_UNKNOWN
	}

	if leftValue.StringValue != nil {
		if rightValue.StringValue == nil {
			return nil, nil, TYPE_UNKNOWN
		}
		return *leftValue.StringValue, *rightValue.StringValue, TYPE_STRING
	}

	if leftValue.BoolValue != nil {
		if rightValue.BoolValue == nil {
			return nil, nil, TYPE_BOOL
		}

		return *leftValue.BoolValue, *rightValue.BoolValue, TYPE_BOOL
	}

	return nil, nil, TYPE_UNKNOWN
}
