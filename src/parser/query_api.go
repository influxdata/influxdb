package parser

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// this file provides the high level api of the query object

var (
	ZERO_TIME = time.Unix(0, 0)
)

func uniq(slice []string) []string {
	// TODO: optimize this, maybe ?
	uniqueMap := map[string]bool{}
	for _, name := range slice {
		uniqueMap[name] = true
	}
	slice = []string{}
	for name, _ := range uniqueMap {
		slice = append(slice, name)
	}
	sort.Strings(slice)
	return slice
}

// Returns a mapping from the time series names (or regex) to the
// column names that are references
func (self *Query) GetReferencedColumns() map[*Value][]string {
	mapping := make(map[string][]string)

	notPrefixedColumns := []string{}
	for _, value := range self.GetColumnNames() {
		notPrefixedColumns = append(notPrefixedColumns, getReferencedColumnsFromValue(value, mapping)...)
	}

	if condition := self.GetWhereCondition(); condition != nil {
		notPrefixedColumns = append(notPrefixedColumns, getReferencedColumnsFromCondition(condition, mapping)...)
	}

	for _, groupBy := range self.GetGroupByClause() {
		notPrefixedColumns = append(notPrefixedColumns, getReferencedColumnsFromValue(groupBy, mapping)...)
	}

	notPrefixedColumns = uniq(notPrefixedColumns)

	returnedMapping := make(map[*Value][]string)
	for _, value := range self.GetFromClause().Names {
		if _, ok := value.GetCompiledRegex(); ok {
			// this is a regex table, cannot be referenced, only unreferenced
			// columns will be attached to regex table names
			returnedMapping[value] = notPrefixedColumns
			continue
		}

		name := value.Name
		returnedMapping[value] = uniq(append(mapping[name], notPrefixedColumns...))
		if len(returnedMapping[value]) > 1 && returnedMapping[value][0] == "*" {
			returnedMapping[value] = returnedMapping[value][:1]
		}

		delete(mapping, name)
	}

	return returnedMapping
}

// Returns the start time of the query. Queries can only have
// one condition of the form time > start_time
func (self *Query) GetStartTime() time.Time {
	return self.startTime
}

// Returns the start time of the query. Queries can only have
// one condition of the form time > start_time
func (self *Query) GetEndTime() time.Time {
	return self.endTime
}

// parse time expressions, e.g. now() - 1d
func parseTime(expr *Expression) (int64, error) {
	if value, ok := expr.GetLeftValue(); ok {
		if value.IsFunctionCall() && value.Name == "now" {
			return time.Now().UnixNano(), nil
		}

		if value.IsFunctionCall() {
			return 0, fmt.Errorf("Invalid use of function %s", value.Name)
		}

		name := value.Name

		parsedInt, err := strconv.ParseInt(name[:len(name)-1], 10, 64)
		if err != nil {
			return 0, err
		}

		switch name[len(name)-1] {
		case 'u':
			return parsedInt * int64(time.Microsecond), nil
		case 's':
			return parsedInt * int64(time.Second), nil
		case 'm':
			return parsedInt * int64(time.Minute), nil
		case 'h':
			return parsedInt * int64(time.Hour), nil
		case 'd':
			return parsedInt * 24 * int64(time.Hour), nil
		case 'w':
			return parsedInt * 7 * 24 * int64(time.Hour), nil
		}

		lastChar := name[len(name)-1]
		if !unicode.IsDigit(rune(lastChar)) {
			return 0, fmt.Errorf("Invalid character '%c'", lastChar)
		}

		extraDigit := int64(lastChar - '0')
		parsedInt = parsedInt*10 + extraDigit
		return int64(parsedInt), err
	}

	leftExpression, _ := expr.GetLeftExpression()
	leftValue, err := parseTime(leftExpression)
	if err != nil {
		return 0, err
	}
	rightValue, err := parseTime(expr.Right)
	if err != nil {
		return 0, err
	}
	switch expr.Operation {
	case '+':
		return leftValue + rightValue, nil
	case '-':
		return leftValue - rightValue, nil
	default:
		return 0, fmt.Errorf("Cannot use '%c' in a time expression", expr.Operation)
	}
}

func getReferencedColumnsFromValue(v *Value, mapping map[string][]string) (notAssigned []string) {
	switch v.Type {
	case ValueSimpleName:
		if idx := strings.LastIndex(v.Name, "."); idx != -1 {
			tableName := v.Name[:idx]
			columnName := v.Name[idx+1:]
			mapping[tableName] = append(mapping[tableName], columnName)
			return
		}
		notAssigned = append(notAssigned, v.Name)
	case ValueWildcard:
		notAssigned = append(notAssigned, "*")
	case ValueFunctionCall:
		for _, value := range v.Elems {
			newNotAssignedColumns := getReferencedColumnsFromValue(value, mapping)
			if len(newNotAssignedColumns) > 0 && newNotAssignedColumns[0] == "*" {
				newNotAssignedColumns = newNotAssignedColumns[1:]
			}

			notAssigned = append(notAssigned, newNotAssignedColumns...)
		}
	}
	return
}

func getReferencedColumnsFromExpression(expr *Expression, mapping map[string][]string) (notAssigned []string) {
	if left, ok := expr.GetLeftExpression(); ok {
		notAssigned = append(notAssigned, getReferencedColumnsFromExpression(left, mapping)...)
		notAssigned = append(notAssigned, getReferencedColumnsFromExpression(expr.Right, mapping)...)
		return
	}

	value, _ := expr.GetLeftValue()
	notAssigned = append(notAssigned, getReferencedColumnsFromValue(value, mapping)...)
	return
}

func getReferencedColumnsFromBool(expr *BoolExpression, mapping map[string][]string) (notAssigned []string) {
	notAssigned = append(notAssigned, getReferencedColumnsFromExpression(expr.Right, mapping)...)
	notAssigned = append(notAssigned, getReferencedColumnsFromExpression(expr.Left, mapping)...)
	return
}

func getReferencedColumnsFromCondition(condition *WhereCondition, mapping map[string][]string) (notPrefixed []string) {
	if left, ok := condition.GetLeftWhereCondition(); ok {
		notPrefixed = append(notPrefixed, getReferencedColumnsFromCondition(left, mapping)...)
		notPrefixed = append(notPrefixed, getReferencedColumnsFromCondition(condition.Right, mapping)...)
		return
	}

	expr, _ := condition.GetBoolExpression()
	notPrefixed = append(notPrefixed, getReferencedColumnsFromBool(expr, mapping)...)
	return
}

// parse the start time or end time from the where conditions and return the new condition
// without the time clauses, or nil if there are no where conditions left
func getTime(condition *WhereCondition, isParsingStartTime bool) (*WhereCondition, time.Time, error) {
	if condition == nil {
		return nil, ZERO_TIME, nil
	}

	if expr, ok := condition.GetBoolExpression(); ok {
		leftValue, isLeftValue := expr.Left.GetLeftValue()
		rightValue, isRightValue := expr.Right.GetLeftValue()

		// if this expression isn't "time > xxx" or "xxx < time" then return
		// TODO: we should do a check to make sure "time" doesn't show up in
		// either expressions
		if !isLeftValue && !isRightValue {
			return condition, ZERO_TIME, nil
		}

		var timeExpression *Expression
		if !isRightValue {
			if leftValue.Name != "time" {
				return condition, ZERO_TIME, nil
			}
			timeExpression = expr.Right
		} else {
			if rightValue.Name != "time" {
				return condition, ZERO_TIME, nil
			}
			timeExpression = expr.Left
		}

		switch expr.Operation {
		case ">":
			if isParsingStartTime && !isLeftValue || !isParsingStartTime && !isRightValue {
				return condition, ZERO_TIME, nil
			}
		case "<":
			if !isParsingStartTime && !isLeftValue || isParsingStartTime && !isRightValue {
				return condition, ZERO_TIME, nil
			}
		default:
			return nil, ZERO_TIME, fmt.Errorf("Cannot use time with '%s'", expr.Operation)
		}

		nanoseconds, err := parseTime(timeExpression)
		if err != nil {
			return nil, ZERO_TIME, err
		}
		return nil, time.Unix(nanoseconds/int64(time.Second), nanoseconds%int64(time.Second)), nil
	}

	leftCondition, _ := condition.GetLeftWhereCondition()
	newLeftCondition, timeLeft, err := getTime(leftCondition, isParsingStartTime)
	if err != nil {
		return nil, ZERO_TIME, err
	}
	newRightCondition, timeRight, err := getTime(condition.Right, isParsingStartTime)
	if err != nil {
		return nil, ZERO_TIME, err
	}

	if condition.Operation == "OR" && (timeLeft != ZERO_TIME || timeRight != ZERO_TIME) {
		// we can't have two start times or'd together
		return nil, ZERO_TIME, fmt.Errorf("Invalid where clause, time must appear twice to specify start and end time")
	}

	newCondition := condition
	if newLeftCondition == nil {
		newCondition = newRightCondition
	} else if newRightCondition == nil {
		newCondition = newLeftCondition
	} else {
		newCondition.Left = newLeftCondition
		newCondition.Right = newRightCondition
	}

	if timeLeft == ZERO_TIME {
		return newCondition, timeRight, nil
	}
	if timeRight == ZERO_TIME {
		return newCondition, timeLeft, nil
	}
	if isParsingStartTime && timeLeft.Unix() < timeRight.Unix() {
		return newCondition, timeLeft, nil
	}
	if !isParsingStartTime && timeLeft.Unix() > timeRight.Unix() {
		return newCondition, timeLeft, nil
	}
	return newCondition, timeRight, nil
}
