package datastore

import (
	"fmt"
	"parser"
	"protocol"
	"strconv"
)

func getExpressionValue(expr *parser.Expression, fields []string, point *protocol.Point) (*protocol.FieldValue, error) {

	if value, ok := expr.GetLeftValue(); ok {
		switch value.Type {
		case parser.ValueFunctionCall:
			return nil, fmt.Errorf("Cannot process function call %s in expression", value.Name)
		case parser.ValueFloat:
			value, _ := strconv.ParseFloat(value.Name, 64)
			return &protocol.FieldValue{DoubleValue: &value}, nil
		case parser.ValueInt:
			value, _ := strconv.ParseInt(value.Name, 10, 64)
			return &protocol.FieldValue{Int64Value: &value}, nil
		case parser.ValueString, parser.ValueRegex:
			return &protocol.FieldValue{StringValue: &value.Name}, nil
		case parser.ValueTableName, parser.ValueSimpleName:

			// TODO: optimize this so we don't have to lookup the column everytime
			fieldIdx := -1
			for idx, field := range fields {
				if field == value.Name {
					fieldIdx = idx
					break
				}
			}

			if fieldIdx == -1 {
				return nil, fmt.Errorf("Cannot find column %s", value.Name)
			}

			return point.Values[fieldIdx], nil
		}
	}

	return nil, fmt.Errorf("Cannot evaluate expression")
}

func matchesExpression(expr *parser.BoolExpression, fields []string, point *protocol.Point) (bool, error) {
	leftValue, err := getExpressionValue(expr.Left, fields, point)
	if err != nil {
		return false, err
	}
	rightValue, err := getExpressionValue(expr.Right, fields, point)
	if err != nil {
		return false, err
	}

	operator := registeredOperators[expr.Operation]
	return operator(leftValue, rightValue)
}

func matches(condition *parser.WhereCondition, fields []string, point *protocol.Point) (bool, error) {
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

func getColumns(values []*parser.Value, columns map[string]bool) {
	for _, v := range values {
		switch v.Type {
		case parser.ValueSimpleName:
			columns[v.Name] = true
		case parser.ValueWildcard:
			columns["*"] = true
			return
		case parser.ValueFunctionCall:
			getColumns(v.Elems, columns)
		}
	}
}

func filterColumns(columns map[string]bool, fields []string, point *protocol.Point) {
	if columns["*"] {
		return
	}

	newValues := []*protocol.FieldValue{}
	newFields := []string{}
	for idx, f := range fields {
		if _, ok := columns[f]; !ok {
			continue
		}

		newValues = append(newValues, point.Values[idx])
		newFields = append(newFields, f)
	}
	point.Values = newValues
}

func Filter(query *parser.Query, series *protocol.Series) (*protocol.Series, error) {
	if query.GetWhereCondition() == nil {
		return series, nil
	}

	columns := map[string]bool{}
	getColumns(query.GetColumnNames(), columns)

	points := series.Points
	series.Points = nil
	for _, point := range points {
		ok, err := matches(query.GetWhereCondition(), series.Fields, point)

		if err != nil {
			return nil, err
		}

		if ok {
			filterColumns(columns, series.Fields, point)
			series.Points = append(series.Points, point)
		}
	}

	if !columns["*"] {
		newFields := []string{}
		for _, f := range series.Fields {
			if _, ok := columns[f]; !ok {
				continue
			}

			newFields = append(newFields, f)
		}
		series.Fields = newFields
	}
	return series, nil
}
