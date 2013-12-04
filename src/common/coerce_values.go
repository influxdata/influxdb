package common

import (
	"protocol"
)

type Type int

const (
	TYPE_INT = iota
	TYPE_STRING
	TYPE_BOOL
	TYPE_DOUBLE
	TYPE_UNKNOWN
)

func CoerceValues(leftValue, rightValue *protocol.FieldValue) (interface{}, interface{}, Type) {
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
