package engine

import (
	"fmt"
	"protocol"
	"regexp"
)

type BooleanOperation func(leftType, rightType protocol.FieldDefinition_Type, leftValue, rightValue *protocol.FieldValue) (bool, error)

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
	registeredOperators["~="] = RegexMatcherOperator
}

func not(op BooleanOperation) BooleanOperation {
	return func(leftType, rightType protocol.FieldDefinition_Type, leftValue, rightValue *protocol.FieldValue) (bool, error) {
		ok, err := op(leftType, rightType, leftValue, rightValue)
		return !ok, err
	}
}

func commonType(leftType, rightType protocol.FieldDefinition_Type) (protocol.FieldDefinition_Type, error) {
	switch leftType {
	case protocol.FieldDefinition_INT64, protocol.FieldDefinition_INT32:
		switch rightType {
		case protocol.FieldDefinition_INT32, protocol.FieldDefinition_INT64:
			return protocol.FieldDefinition_INT64, nil
		case protocol.FieldDefinition_DOUBLE:
			return protocol.FieldDefinition_DOUBLE, nil
		}

	case protocol.FieldDefinition_DOUBLE:
		switch rightType {
		case protocol.FieldDefinition_INT64, protocol.FieldDefinition_INT32, protocol.FieldDefinition_DOUBLE:
			return protocol.FieldDefinition_DOUBLE, nil
		}

	case protocol.FieldDefinition_BOOL:
		if rightType == protocol.FieldDefinition_BOOL {
			return protocol.FieldDefinition_BOOL, nil
		}

	case protocol.FieldDefinition_STRING:
		if rightType == protocol.FieldDefinition_STRING {
			return protocol.FieldDefinition_STRING, nil
		}
	}

	return 0, fmt.Errorf("%v and %v cannot be coerced to a common type", leftType, rightType)
}

func coerceValue(value *protocol.FieldValue, fromType, toType protocol.FieldDefinition_Type) *protocol.FieldValue {
	switch toType {
	case protocol.FieldDefinition_INT64:
		switch fromType {
		case protocol.FieldDefinition_INT64:
			return value
		case protocol.FieldDefinition_INT32:
			temp := int64(*value.IntValue)
			return &protocol.FieldValue{Int64Value: &temp}
		}

	case protocol.FieldDefinition_DOUBLE:
		switch fromType {
		case protocol.FieldDefinition_DOUBLE:
			return value
		case protocol.FieldDefinition_INT64:
			temp := float64(*value.Int64Value)
			return &protocol.FieldValue{DoubleValue: &temp}
		case protocol.FieldDefinition_INT32:
			temp := float64(*value.IntValue)
			return &protocol.FieldValue{DoubleValue: &temp}
		}

	}

	return value
}

func EqualityOperator(leftType, rightType protocol.FieldDefinition_Type, leftValue, rightValue *protocol.FieldValue) (bool, error) {
	cType, err := commonType(leftType, rightType)
	if err != nil {
		return false, err
	}

	leftValue = coerceValue(leftValue, leftType, cType)
	rightValue = coerceValue(rightValue, rightType, cType)

	switch cType {
	case protocol.FieldDefinition_STRING:
		return *leftValue.StringValue == *rightValue.StringValue, nil
	case protocol.FieldDefinition_INT64:
		return *leftValue.Int64Value == *rightValue.Int64Value, nil
	case protocol.FieldDefinition_DOUBLE:
		return *leftValue.DoubleValue == *rightValue.DoubleValue, nil
	case protocol.FieldDefinition_BOOL:
		return *leftValue.BoolValue == *rightValue.BoolValue, nil
	default:
		return false, fmt.Errorf("Unknown type %v", cType)
	}
}

func RegexMatcherOperator(leftType, rightType protocol.FieldDefinition_Type, leftValue, rightValue *protocol.FieldValue) (bool, error) {
	cType, err := commonType(leftType, rightType)
	if err != nil {
		return false, err
	}

	leftValue = coerceValue(leftValue, leftType, cType)
	rightValue = coerceValue(rightValue, rightType, cType)

	switch cType {
	case protocol.FieldDefinition_STRING:
		// TODO: do case insensitive matching
		return regexp.MatchString(*rightValue.StringValue, *leftValue.StringValue)
	default:
		return false, fmt.Errorf("Cannot use regex matcher with type %v", cType)
	}
}

func GreaterThanOrEqualOperator(leftType, rightType protocol.FieldDefinition_Type, leftValue, rightValue *protocol.FieldValue) (bool, error) {
	cType, err := commonType(leftType, rightType)
	if err != nil {
		return false, err
	}

	leftValue = coerceValue(leftValue, leftType, cType)
	rightValue = coerceValue(rightValue, rightType, cType)

	switch cType {
	case protocol.FieldDefinition_STRING:
		return *leftValue.StringValue >= *rightValue.StringValue, nil
	case protocol.FieldDefinition_INT64:
		return *leftValue.Int64Value >= *rightValue.Int64Value, nil
	case protocol.FieldDefinition_DOUBLE:
		return *leftValue.DoubleValue >= *rightValue.DoubleValue, nil
	default:
		return false, fmt.Errorf("Cannot use >= with type %v", cType)
	}
}

func GreaterThanOperator(leftType, rightType protocol.FieldDefinition_Type, leftValue, rightValue *protocol.FieldValue) (bool, error) {
	cType, err := commonType(leftType, rightType)
	if err != nil {
		return false, err
	}

	leftValue = coerceValue(leftValue, leftType, cType)
	rightValue = coerceValue(rightValue, rightType, cType)

	switch cType {
	case protocol.FieldDefinition_STRING:
		return *leftValue.StringValue > *rightValue.StringValue, nil
	case protocol.FieldDefinition_INT64:
		return *leftValue.Int64Value > *rightValue.Int64Value, nil
	case protocol.FieldDefinition_DOUBLE:
		return *leftValue.DoubleValue > *rightValue.DoubleValue, nil
	default:
		return false, fmt.Errorf("Cannot use > with type %v", cType)
	}
}
