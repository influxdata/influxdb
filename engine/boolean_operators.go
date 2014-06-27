package engine

import (
	"fmt"
	"regexp"

	"github.com/influxdb/influxdb/common"
	"github.com/influxdb/influxdb/protocol"
)

type OperatorResult int

const (
	MATCH OperatorResult = iota
	NO_MATCH
	INVALID
)

type oldBooleanOperation func(leftValue, rightValues *protocol.FieldValue) (OperatorResult, error)
type BooleanOperation func(leftValue *protocol.FieldValue, rightValues []*protocol.FieldValue) (OperatorResult, error)

func wrapOldBooleanOperation(operation oldBooleanOperation) BooleanOperation {
	return func(leftValue *protocol.FieldValue, rightValues []*protocol.FieldValue) (OperatorResult, error) {
		if len(rightValues) != 1 {
			return INVALID, fmt.Errorf("Expected one value on the right side")
		}

		if leftValue == nil || rightValues[0] == nil {
			return INVALID, nil
		}

		return operation(leftValue, rightValues[0])
	}
}

var (
	registeredOperators = map[string]BooleanOperation{}
)

func init() {
	registeredOperators["="] = wrapOldBooleanOperation(EqualityOperator)
	registeredOperators["<>"] = not(wrapOldBooleanOperation(EqualityOperator))
	registeredOperators[">="] = wrapOldBooleanOperation(GreaterThanOrEqualOperator)
	registeredOperators[">"] = wrapOldBooleanOperation(GreaterThanOperator)
	registeredOperators["<"] = not(wrapOldBooleanOperation(GreaterThanOrEqualOperator))
	registeredOperators["<="] = not(wrapOldBooleanOperation(GreaterThanOperator))
	registeredOperators["=~"] = wrapOldBooleanOperation(RegexMatcherOperator)
	registeredOperators["!~"] = not(wrapOldBooleanOperation(RegexMatcherOperator))
	registeredOperators["in"] = InOperator
}

func not(op BooleanOperation) BooleanOperation {
	return func(leftValue *protocol.FieldValue, rightValue []*protocol.FieldValue) (OperatorResult, error) {
		ok, err := op(leftValue, rightValue)
		switch ok {
		case MATCH:
			return NO_MATCH, err
		case NO_MATCH:
			return MATCH, err
		default:
			return INVALID, err
		}
	}
}

func EqualityOperator(leftValue, rightValue *protocol.FieldValue) (OperatorResult, error) {
	v1, v2, cType := common.CoerceValues(leftValue, rightValue)

	switch cType {
	case common.TYPE_STRING:
		if v1.(string) == v2.(string) {
			return MATCH, nil
		}
		return NO_MATCH, nil
	case common.TYPE_INT:
		if v1.(int64) == v2.(int64) {
			return MATCH, nil
		}
		return NO_MATCH, nil
	case common.TYPE_DOUBLE:
		if v1.(float64) == v2.(float64) {
			return MATCH, nil
		}
		return NO_MATCH, nil
	case common.TYPE_BOOL:
		if v1.(bool) == v2.(bool) {
			return MATCH, nil
		}
		return NO_MATCH, nil
	default:
		return INVALID, nil
	}
}

func RegexMatcherOperator(leftValue, rightValue *protocol.FieldValue) (OperatorResult, error) {
	v1, v2, cType := common.CoerceValues(leftValue, rightValue)

	switch cType {
	case common.TYPE_STRING:
		// TODO: assume that the regex is valid
		if ok, _ := regexp.MatchString(v2.(string), v1.(string)); ok {
			return MATCH, nil
		}
		return NO_MATCH, nil
	default:
		return INVALID, nil
	}
}

func GreaterThanOrEqualOperator(leftValue, rightValue *protocol.FieldValue) (OperatorResult, error) {
	v1, v2, cType := common.CoerceValues(leftValue, rightValue)

	switch cType {
	case common.TYPE_STRING:
		if v1.(string) >= v2.(string) {
			return MATCH, nil
		}
		return NO_MATCH, nil
	case common.TYPE_INT:
		if v1.(int64) >= v2.(int64) {
			return MATCH, nil
		}
		return NO_MATCH, nil
	case common.TYPE_DOUBLE:
		if v1.(float64) >= v2.(float64) {
			return MATCH, nil
		}
		return NO_MATCH, nil
	default:
		return INVALID, nil
	}
}

func GreaterThanOperator(leftValue, rightValue *protocol.FieldValue) (OperatorResult, error) {
	v1, v2, cType := common.CoerceValues(leftValue, rightValue)

	switch cType {
	case common.TYPE_STRING:
		if v1.(string) > v2.(string) {
			return MATCH, nil
		}
		return NO_MATCH, nil
	case common.TYPE_INT:
		if v1.(int64) > v2.(int64) {
			return MATCH, nil
		}
		return NO_MATCH, nil
	case common.TYPE_DOUBLE:
		if v1.(float64) > v2.(float64) {
			return MATCH, nil
		}
		return NO_MATCH, nil
	default:
		return INVALID, nil
	}
}

func InOperator(leftValue *protocol.FieldValue, rightValue []*protocol.FieldValue) (OperatorResult, error) {
	for _, v := range rightValue {
		v1, v2, cType := common.CoerceValues(leftValue, v)

		var result bool

		switch cType {
		case common.TYPE_STRING:
			result = v1.(string) == v2.(string)
		case common.TYPE_INT:
			result = v1.(int64) == v2.(int64)
		case common.TYPE_DOUBLE:
			result = v1.(float64) == v2.(float64)
		case common.TYPE_BOOL:
			result = v1.(bool) == v2.(bool)
		default:
			return INVALID, nil
		}

		if result {
			return MATCH, nil
		}
	}

	return NO_MATCH, nil
}
