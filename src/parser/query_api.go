package parser

import (
	"fmt"
	"regexp"
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

func (self *Query) WillReturnSingleSeries() bool {
	fromClause := self.GetFromClause()
	if fromClause.Type != FromClauseArray {
		return false
	}

	if len(fromClause.Names) > 1 {
		return false
	}

	if _, ok := fromClause.Names[0].Name.GetCompiledRegex(); ok {
		return false
	}

	return true
}

func (self *Query) GetTableAliases(name string) []string {
	names := self.GetFromClause().Names
	if len(names) == 1 && names[0].Name.Type == ValueRegex {
		return []string{name}
	}

	aliases := []string{}

	for _, fromName := range names {
		if fromName.Name.Name != name {
			continue
		}

		if fromName.Alias == "" {
			aliases = append(aliases, name)
			continue
		}

		aliases = append(aliases, fromName.Alias)
	}
	return aliases
}

func (self *Query) revertAlias(mapping map[string][]string) {
	fromClause := self.GetFromClause()
	if fromClause.Type != FromClauseInnerJoin {
		return
	}

	columns := make(map[string]map[string]bool)

	for _, table := range fromClause.Names {
		name := table.Name.Name
		alias := name
		if table.Alias != "" {
			alias = table.Alias
		}

		for _, column := range mapping[alias] {
			tableColumns := columns[name]
			if tableColumns == nil {
				tableColumns = make(map[string]bool)
				columns[name] = tableColumns
			}
			tableColumns[column] = true
		}

		delete(mapping, alias)
	}

	for table, tableColumns := range columns {
		mapping[table] = []string{}
		for column, _ := range tableColumns {
			mapping[table] = append(mapping[table], column)
		}
	}
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

	self.revertAlias(mapping)

	addedTables := make(map[string]bool)

	returnedMapping := make(map[*Value][]string)
	for _, tableName := range self.GetFromClause().Names {
		value := tableName.Name
		if _, ok := value.GetCompiledRegex(); ok {
			// this is a regex table, cannot be referenced, only unreferenced
			// columns will be attached to regex table names
			returnedMapping[value] = notPrefixedColumns
			continue
		}

		name := value.Name
		if addedTables[name] {
			continue
		}
		addedTables[name] = true
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

// parse time that matches the following format:
//   2006-01-02 [15[:04[:05[.000]]]]
// notice, hour, minute and seconds are optional
var time_regex *regexp.Regexp

func init() {
	var err error
	time_regex, err = regexp.Compile(
		"^([0-9]{4}|[0-9]{2})-[0-9]{1,2}-[0-9]{1,2}( [0-9]{1,2}(:[0-9]{1,2}(:[0-9]{1,2}?(\\.[0-9]+)?)?)?)?$")
	if err != nil {
		panic(err)
	}
}

func parseTimeString(t string) (time.Time, error) {
	submatches := time_regex.FindStringSubmatch(t)
	if len(submatches) == 0 {
		return ZERO_TIME, fmt.Errorf("%s isn't a valid time string", t)
	}

	if submatches[5] != "" || submatches[4] != "" {
		return time.Parse("2006-01-02 15:04:05", t)
	}

	if submatches[3] != "" {
		return time.Parse("2006-01-02 15:04", t)
	}

	if submatches[2] != "" {
		return time.Parse("2006-01-02 15", t)
	}

	return time.Parse("2006-01-02", t)
}

// parse time expressions, e.g. now() - 1d
func parseTime(value *Value) (int64, error) {
	if value.Type != ValueExpression {
		if value.IsFunctionCall() && value.Name == "now" {
			return time.Now().UnixNano(), nil
		}

		if value.IsFunctionCall() {
			return 0, fmt.Errorf("Invalid use of function %s", value.Name)
		}

		if value.Type == ValueString {
			t, err := parseTimeString(value.Name)
			return t.UnixNano(), err
		}

		name := value.Name

		parsedFloat, err := strconv.ParseFloat(name[:len(name)-1], 64)
		if err != nil {
			return 0, err
		}

		switch name[len(name)-1] {
		case 'u':
			return int64(parsedFloat * float64(time.Microsecond)), nil
		case 's':
			return int64(parsedFloat * float64(time.Second)), nil
		case 'm':
			return int64(parsedFloat * float64(time.Minute)), nil
		case 'h':
			return int64(parsedFloat * float64(time.Hour)), nil
		case 'd':
			return int64(parsedFloat * 24 * float64(time.Hour)), nil
		case 'w':
			return int64(parsedFloat * 7 * 24 * float64(time.Hour)), nil
		}

		lastChar := name[len(name)-1]
		if !unicode.IsDigit(rune(lastChar)) && lastChar != '.' {
			return 0, fmt.Errorf("Invalid character '%c'", lastChar)
		}

		if name[len(name)-2] != '.' {
			extraDigit := float64(lastChar - '0')
			parsedFloat = parsedFloat*10 + extraDigit
		}
		return int64(parsedFloat), nil
	}

	leftValue, err := parseTime(value.Elems[0])
	if err != nil {
		return 0, err
	}
	rightValue, err := parseTime(value.Elems[1])
	if err != nil {
		return 0, err
	}
	switch value.Name {
	case "+":
		return leftValue + rightValue, nil
	case "-":
		return leftValue - rightValue, nil
	default:
		return 0, fmt.Errorf("Cannot use '%s' in a time expression", value.Name)
	}
}

func getReferencedColumnsFromValue(v *Value, mapping map[string][]string) (notAssigned []string) {
	switch v.Type {
	case ValueSimpleName, ValueTableName:
		if idx := strings.LastIndex(v.Name, "."); idx != -1 {
			tableName := v.Name[:idx]
			columnName := v.Name[idx+1:]
			mapping[tableName] = append(mapping[tableName], columnName)
			return
		}
		notAssigned = append(notAssigned, v.Name)
	case ValueWildcard:
		notAssigned = append(notAssigned, "*")
	case ValueExpression, ValueFunctionCall:
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

func getReferencedColumnsFromCondition(condition *WhereCondition, mapping map[string][]string) (notPrefixed []string) {
	if left, ok := condition.GetLeftWhereCondition(); ok {
		notPrefixed = append(notPrefixed, getReferencedColumnsFromCondition(left, mapping)...)
		notPrefixed = append(notPrefixed, getReferencedColumnsFromCondition(condition.Right, mapping)...)
		return
	}

	expr, _ := condition.GetBoolExpression()
	notPrefixed = append(notPrefixed, getReferencedColumnsFromValue(expr, mapping)...)
	return
}

func isNumericValue(value *Value) bool {
	switch value.Type {
	case ValueDuration, ValueFloat, ValueInt, ValueString:
		return true
	default:
		return false
	}
}

// parse the start time or end time from the where conditions and return the new condition
// without the time clauses, or nil if there are no where conditions left
func getTime(condition *WhereCondition, isParsingStartTime bool) (*WhereCondition, time.Time, error) {
	if condition == nil {
		return nil, ZERO_TIME, nil
	}

	if expr, ok := condition.GetBoolExpression(); ok {
		leftValue := expr.Elems[0]
		isLeftValue := leftValue.Type != ValueExpression
		rightValue := expr.Elems[1]
		isRightValue := rightValue.Type != ValueExpression

		// this can only be the case if the where condition
		// is of the form `"time" > 123456789`, so let's see
		// which side is a float value
		if isLeftValue && isRightValue {
			if isNumericValue(rightValue) {
				isRightValue = false
			} else {
				isLeftValue = false
			}
		}

		// if this expression isn't "time > xxx" or "xxx < time" then return
		// TODO: we should do a check to make sure "time" doesn't show up in
		// either expressions
		if !isLeftValue && !isRightValue {
			return condition, ZERO_TIME, nil
		}

		var timeExpression *Value
		if !isRightValue {
			if leftValue.Name != "time" {
				return condition, ZERO_TIME, nil
			}
			timeExpression = rightValue
		} else if !isLeftValue {
			if rightValue.Name != "time" {
				return condition, ZERO_TIME, nil
			}
			timeExpression = leftValue
		} else {
			return nil, ZERO_TIME, fmt.Errorf("Invalid time condition %v", condition)
		}

		switch expr.Name {
		case ">":
			if isParsingStartTime && !isLeftValue || !isParsingStartTime && !isRightValue {
				return condition, ZERO_TIME, nil
			}
		case "<":
			if !isParsingStartTime && !isLeftValue || isParsingStartTime && !isRightValue {
				return condition, ZERO_TIME, nil
			}
		default:
			return nil, ZERO_TIME, fmt.Errorf("Cannot use time with '%s'", expr.Name)
		}

		nanoseconds, err := parseTime(timeExpression)
		if err != nil {
			return nil, ZERO_TIME, err
		}
		return nil, time.Unix(nanoseconds/int64(time.Second), nanoseconds%int64(time.Second)).UTC(), nil
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
