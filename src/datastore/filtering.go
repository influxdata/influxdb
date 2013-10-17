package datastore

import (
	"fmt"
	"parser"
	"protocol"
	"strconv"
)

func getExpressionValue(expr *parser.Expression, fields []*protocol.FieldDefinition, point *protocol.Point) (
	protocol.FieldDefinition_Type, *protocol.FieldValue, error) {

	if value, ok := expr.GetLeftValue(); ok {
		switch value.Type {
		case parser.ValueFunctionCall:
			return 0, nil, fmt.Errorf("Cannot process function call %s in expression", value.Name)
		case parser.ValueFloat:
			value, _ := strconv.ParseFloat(value.Name, 64)
			return protocol.FieldDefinition_DOUBLE, &protocol.FieldValue{DoubleValue: &value}, nil
		case parser.ValueInt:
			value, _ := strconv.ParseInt(value.Name, 10, 64)
			return protocol.FieldDefinition_INT64, &protocol.FieldValue{Int64Value: &value}, nil
		case parser.ValueString, parser.ValueRegex:
			return protocol.FieldDefinition_STRING, &protocol.FieldValue{StringValue: &value.Name}, nil
			return protocol.FieldDefinition_STRING, &protocol.FieldValue{StringValue: &value.Name}, nil
		case parser.ValueSimpleName:
			// TODO: optimize this so we don't have to lookup the column everytime
			fieldIdx := -1
			for idx, field := range fields {
				if *field.Name == value.Name {
					fieldIdx = idx
					break
				}
			}

			if fieldIdx == -1 {
				return 0, nil, fmt.Errorf("Cannot find column %s", value.Name)
			}

			return *fields[fieldIdx].Type, point.Values[fieldIdx], nil
		}
	}

	return 0, nil, fmt.Errorf("Cannot evaulate expression")
}

func matchesExpression(expr *parser.BoolExpression, fields []*protocol.FieldDefinition, point *protocol.Point) (bool, error) {
	leftType, leftValue, err := getExpressionValue(expr.Left, fields, point)
	if err != nil {
		return false, err
	}
	rightType, rightValue, err := getExpressionValue(expr.Right, fields, point)
	if err != nil {
		return false, err
	}
	operator := registeredOperators[expr.Operation]
	return operator(leftType, rightType, leftValue, rightValue)
}

func matches(condition *parser.WhereCondition, fields []*protocol.FieldDefinition, point *protocol.Point) (bool, error) {
	if expr, ok := condition.GetBoolExpression(); ok {
		return matchesExpression(expr, fields, point)
	}

	left, _ := condition.GetLeftWhereCondition()
	leftResult, err := matches(left, fields, point)
	if err != nil {
		return false, err
	}

	// short circuit
	if !leftResult && condition.Operation == "AND" ||
		leftResult && condition.Operation == "OR" {
		return leftResult, nil
	}

	return matches(condition.Right, fields, point)
}

func Filter(query *parser.Query, series *protocol.Series) (*protocol.Series, error) {
	if query.GetWhereCondition() == nil {
		return series, nil
	}

	points := series.Points
	series.Points = nil
	for _, point := range points {
		ok, err := matches(query.GetWhereCondition(), series.Fields, point)

		if err != nil {
			return nil, err
		}

		if ok {
			series.Points = append(series.Points, point)
		}
	}
	return series, nil
}
