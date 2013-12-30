package datastore

import (
	"fmt"
	"parser"
	"protocol"
	"strconv"
)

func getExpressionValue(values []*parser.Value, fields []string, point *protocol.Point) ([]*protocol.FieldValue, error) {
	fieldValues := []*protocol.FieldValue{}
	for _, value := range values {
		switch value.Type {
		case parser.ValueFunctionCall:
			return nil, fmt.Errorf("Cannot process function call %s in expression", value.Name)
		case parser.ValueFloat:
			value, _ := strconv.ParseFloat(value.Name, 64)
			fieldValues = append(fieldValues, &protocol.FieldValue{DoubleValue: &value})
		case parser.ValueInt:
			value, _ := strconv.ParseInt(value.Name, 10, 64)
			fieldValues = append(fieldValues, &protocol.FieldValue{Int64Value: &value})
		case parser.ValueString, parser.ValueRegex:
			fieldValues = append(fieldValues, &protocol.FieldValue{StringValue: &value.Name})

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
			fieldValues = append(fieldValues, point.Values[fieldIdx])

		case parser.ValueExpression:
			v, err := GetValue(value, fields, point)
			if err != nil {
				return nil, err
			}
			fieldValues = append(fieldValues, v)
		default:
			return nil, fmt.Errorf("Cannot evaluate expression")
		}
	}

	return fieldValues, nil
}

func matchesExpression(expr *parser.Value, fields []string, point *protocol.Point) (bool, error) {
	leftValue, err := getExpressionValue(expr.Elems[:1], fields, point)
	if err != nil {
		return false, err
	}
	rightValue, err := getExpressionValue(expr.Elems[1:], fields, point)
	if err != nil {
		return false, err
	}

	operator := registeredOperators[expr.Name]
	return operator(leftValue[0], rightValue)
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

func Filter(query *parser.SelectQuery, series *protocol.Series) (*protocol.Series, error) {
	if query.GetWhereCondition() == nil {
		return series, nil
	}

	columns := map[string]bool{}
	getColumns(query.GetColumnNames(), columns)
	getColumns(query.GetGroupByClause().Elems, columns)

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
