package datastore

import (
	"protocol"
	"regexp"
)

type BooleanOperation func(leftValue, rightValue *protocol.FieldValue) (bool, error)

var (
	registeredOperators = map[string]BooleanOperation{}
)

func init() {
	registeredOperators["=="] = EqualityOperator
	registeredOperators["!="] = not(EqualityOperator)
	registeredOperators[">="] = GreaterThanOrEqualOperator
	registeredOperators[">"] = GreaterThanOperator
	registeredOperators["<"] = not(GreaterThanOrEqualOperator)
	registeredOperators["<="] = not(GreaterThanOperator)
	registeredOperators["=~"] = RegexMatcherOperator
	registeredOperators["!~"] = not(RegexMatcherOperator)
}

func not(op BooleanOperation) BooleanOperation {
	return func(leftValue, rightValue *protocol.FieldValue) (bool, error) {
		ok, err := op(leftValue, rightValue)
		return !ok, err
	}
}

type Type int

const (
	TYPE_INT = iota
	TYPE_STRING
	TYPE_BOOL
	TYPE_DOUBLE
	TYPE_UNKNOWN
)

func coerceValues(leftValue, rightValue *protocol.FieldValue) (interface{}, interface{}, Type) {
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

func EqualityOperator(leftValue, rightValue *protocol.FieldValue) (bool, error) {
	v1, v2, cType := coerceValues(leftValue, rightValue)

	switch cType {
	case TYPE_STRING:
		return v1.(string) == v2.(string), nil
	case TYPE_INT:
		return v1.(int64) == v2.(int64), nil
	case TYPE_DOUBLE:
		return v1.(float64) == v2.(float64), nil
	case TYPE_BOOL:
		return v1.(bool) == v2.(bool), nil
	default:
		return false, nil
	}
}

func RegexMatcherOperator(leftValue, rightValue *protocol.FieldValue) (bool, error) {
	v1, v2, cType := coerceValues(leftValue, rightValue)

	switch cType {
	case TYPE_STRING:
		// TODO: assume that the regex is valid
		matches, _ := regexp.MatchString(v2.(string), v1.(string))
		return matches, nil
	default:
		return false, nil
	}
}

func GreaterThanOrEqualOperator(leftValue, rightValue *protocol.FieldValue) (bool, error) {
	v1, v2, cType := coerceValues(leftValue, rightValue)

	switch cType {
	case TYPE_STRING:
		return v1.(string) >= v2.(string), nil
	case TYPE_INT:
		return v1.(int64) >= v2.(int64), nil
	case TYPE_DOUBLE:
		return v1.(float64) >= v2.(float64), nil
	default:
		return false, nil
	}
}

func GreaterThanOperator(leftValue, rightValue *protocol.FieldValue) (bool, error) {
	v1, v2, cType := coerceValues(leftValue, rightValue)

	switch cType {
	case TYPE_STRING:
		return v1.(string) > v2.(string), nil
	case TYPE_INT:
		return v1.(int64) > v2.(int64), nil
	case TYPE_DOUBLE:
		return v1.(float64) > v2.(float64), nil
	default:
		return false, nil
	}
}
