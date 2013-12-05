package datastore

import (
	"common"
	"fmt"
	"protocol"
	"regexp"
)

type oldBooleanOperation func(leftValue, rightValues *protocol.FieldValue) (bool, error)
type BooleanOperation func(leftValue *protocol.FieldValue, rightValues []*protocol.FieldValue) (bool, error)

func wrapOldBooleanOperation(operation oldBooleanOperation) BooleanOperation {
	return func(leftValue *protocol.FieldValue, rightValues []*protocol.FieldValue) (bool, error) {
		if len(rightValues) != 1 {
			return false, fmt.Errorf("Expected one value on the right side")
		}

		if leftValue == nil || rightValues[0] == nil {
			return false, nil
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
	return func(leftValue *protocol.FieldValue, rightValue []*protocol.FieldValue) (bool, error) {
		ok, err := op(leftValue, rightValue)
		return !ok, err
	}
}

func EqualityOperator(leftValue, rightValue *protocol.FieldValue) (bool, error) {
	v1, v2, cType := common.CoerceValues(leftValue, rightValue)

	switch cType {
	case common.TYPE_STRING:
		return v1.(string) == v2.(string), nil
	case common.TYPE_INT:
		return v1.(int64) == v2.(int64), nil
	case common.TYPE_DOUBLE:
		return v1.(float64) == v2.(float64), nil
	case common.TYPE_BOOL:
		return v1.(bool) == v2.(bool), nil
	default:
		return false, nil
	}
}

func RegexMatcherOperator(leftValue, rightValue *protocol.FieldValue) (bool, error) {
	v1, v2, cType := common.CoerceValues(leftValue, rightValue)

	switch cType {
	case common.TYPE_STRING:
		// TODO: assume that the regex is valid
		matches, _ := regexp.MatchString(v2.(string), v1.(string))
		return matches, nil
	default:
		return false, nil
	}
}

func GreaterThanOrEqualOperator(leftValue, rightValue *protocol.FieldValue) (bool, error) {
	v1, v2, cType := common.CoerceValues(leftValue, rightValue)

	switch cType {
	case common.TYPE_STRING:
		return v1.(string) >= v2.(string), nil
	case common.TYPE_INT:
		return v1.(int64) >= v2.(int64), nil
	case common.TYPE_DOUBLE:
		return v1.(float64) >= v2.(float64), nil
	default:
		return false, nil
	}
}

func GreaterThanOperator(leftValue, rightValue *protocol.FieldValue) (bool, error) {
	v1, v2, cType := common.CoerceValues(leftValue, rightValue)

	switch cType {
	case common.TYPE_STRING:
		return v1.(string) > v2.(string), nil
	case common.TYPE_INT:
		return v1.(int64) > v2.(int64), nil
	case common.TYPE_DOUBLE:
		return v1.(float64) > v2.(float64), nil
	default:
		return false, nil
	}
}

func InOperator(leftValue *protocol.FieldValue, rightValue []*protocol.FieldValue) (bool, error) {
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
			result = false
		}

		if result {
			return true, nil
		}
	}

	return false, nil
}
