package parser

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/influxdb/influxdb/common"
)

// this file provides the high level api of the query object

func uniq(slice []string) []string {
	// TODO: optimize this, maybe ?
	uniqueMap := map[string]bool{}
	for _, name := range slice {
		uniqueMap[name] = true
	}
	slice = []string{}
	for name := range uniqueMap {
		slice = append(slice, name)
	}
	sort.Strings(slice)
	return slice
}

func (self *SelectDeleteCommonQuery) WillReturnSingleSeries() bool {
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

func (self *SelectDeleteCommonQuery) GetTableAliases(name string) []string {
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

func (self *SelectQuery) revertAlias(mapping map[string][]string) {
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
		for column := range tableColumns {
			mapping[table] = append(mapping[table], column)
		}
	}
}

// Returns true if the query has aggregate functions applied to the
// columns
func (self *SelectQuery) HasAggregates() bool {
	for _, column := range self.GetColumnNames() {
		if column.IsFunctionCall() {
			return true
		}
	}
	return false
}

// Returns a mapping from the time series names (or regex) to the
// column names that are references
func (self *SelectQuery) GetReferencedColumns() map[*Value][]string {
	return self.getColumns(true)
}

func (self *SelectQuery) GetResultColumns() map[*Value][]string {
	return self.getColumns(false)
}

func (self *SelectQuery) getColumns(includeWhereClause bool) map[*Value][]string {
	mapping := make(map[string][]string)

	notPrefixedColumns := []string{}
	for _, value := range self.GetColumnNames() {
		if value.Name == "time" || value.Name == "sequence_number" {
			continue
		}
		notPrefixedColumns = append(notPrefixedColumns, getReferencedColumnsFromValue(value, mapping)...)
	}

	if !self.IsSinglePointQuery() {
		if condition := self.GetWhereCondition(); condition != nil && includeWhereClause {
			notPrefixedColumns = append(notPrefixedColumns, getReferencedColumnsFromCondition(condition, mapping)...)
		}

		for _, groupBy := range self.groupByClause.Elems {
			notPrefixedColumns = append(notPrefixedColumns, getReferencedColumnsFromValue(groupBy, mapping)...)
		}
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

	if len(mapping) == 0 {
		return returnedMapping
	}

	// if `mapping` still have some mappings, then we have mistaken a
	// column name with dots with a prefix.column, see issue #240
	for prefix, columnNames := range mapping {
		for _, columnName := range columnNames {
			for table, columns := range returnedMapping {
				if len(returnedMapping[table]) > 1 && returnedMapping[table][0] == "*" {
					continue
				}
				returnedMapping[table] = append(columns, prefix+"."+columnName)
			}
		}
		delete(mapping, prefix)
	}

	return returnedMapping
}

// Returns the start time of the query. Queries can only have
// one condition of the form time > start_time
func (self *BasicQuery) GetStartTime() time.Time {
	return self.startTime
}

func (self *BasicQuery) IsStartTimeSpecified() bool {
	return self.startTimeSpecified
}

// Returns the start time of the query. Queries can only have
// one condition of the form time > start_time
func (self *BasicQuery) GetEndTime() time.Time {
	return self.endTime
}

// parse time that matches the following format:
//   2006-01-02 [15[:04[:05[.000]]]]
// notice, hour, minute and seconds are optional
var timeRegex *regexp.Regexp

func init() {
	var err error
	timeRegex, err = regexp.Compile(
		"^([0-9]{4}|[0-9]{2})-[0-9]{1,2}-[0-9]{1,2}( [0-9]{1,2}(:[0-9]{1,2}(:[0-9]{1,2}?(\\.[0-9]+)?)?)?)?$")
	if err != nil {
		panic(err)
	}
}

func parseTimeString(t string) (*time.Time, error) {
	submatches := timeRegex.FindStringSubmatch(t)
	if len(submatches) == 0 {
		return nil, fmt.Errorf("%s isn't a valid time string", t)
	}

	if submatches[5] != "" || submatches[4] != "" {
		t, err := time.Parse("2006-01-02 15:04:05", t)
		return &t, err
	}

	if submatches[3] != "" {
		t, err := time.Parse("2006-01-02 15:04", t)
		return &t, err
	}

	if submatches[2] != "" {
		t, err := time.Parse("2006-01-02 15", t)
		return &t, err
	}

	_t, err := time.Parse("2006-01-02", t)
	return &_t, err
}

func parseTimeWithoutSuffix(value string) (int64, error) {
	var err error
	var f float64
	var i int64
	if strings.Contains(value, ".") {
		f, err = strconv.ParseFloat(value, 64)
		i = int64(f)
	} else {
		i, err = strconv.ParseInt(value, 10, 64)
	}
	if err != nil {
		return 0, err
	}
	return i, nil
}

// parse time expressions, e.g. now() - 1d
func parseTime(value *Value) (int64, error) {
	if value.Type != ValueExpression {
		if value.IsFunctionCall() && strings.ToLower(value.Name) == "now" {
			return time.Now().UTC().UnixNano(), nil
		}

		if value.IsFunctionCall() {
			return 0, fmt.Errorf("Invalid use of function %s", value.Name)
		}

		if value.Type == ValueString {
			t, err := parseTimeString(value.Name)
			if err != nil {
				return 0, err
			}
			return t.UnixNano(), err
		}

		return common.ParseTimeDuration(value.Name)
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
func getTime(condition *WhereCondition, isParsingStartTime bool) (*WhereCondition, *time.Time, error) {
	if condition == nil {
		return nil, nil, nil
	}

	if expr, ok := condition.GetBoolExpression(); ok {
		switch expr.Type {
		case ValueDuration, ValueFloat, ValueInt, ValueString, ValueWildcard:
			return nil, nil, fmt.Errorf("Invalid where expression: %v", expr)
		}

		if expr.Type == ValueFunctionCall {
			return condition, nil, nil
		}

		leftValue := expr.Elems[0]
		isTimeOnLeft := leftValue.Type != ValueExpression && leftValue.Type != ValueFunctionCall
		rightValue := expr.Elems[1]
		isTimeOnRight := rightValue.Type != ValueExpression && rightValue.Type != ValueFunctionCall

		// this can only be the case if the where condition
		// is of the form `"time" > 123456789`, so let's see
		// which side is a float value
		if isTimeOnLeft && isTimeOnRight {
			if isNumericValue(rightValue) {
				isTimeOnRight = false
			} else {
				isTimeOnLeft = false
			}
		}

		// if this expression isn't "time > xxx" or "xxx < time" then return
		// TODO: we should do a check to make sure "time" doesn't show up in
		// either expressions
		if !isTimeOnLeft && !isTimeOnRight {
			return condition, nil, nil
		}

		var timeExpression *Value
		if !isTimeOnRight {
			if leftValue.Name != "time" {
				return condition, nil, nil
			}
			timeExpression = rightValue
		} else if !isTimeOnLeft {
			if rightValue.Name != "time" {
				return condition, nil, nil
			}
			timeExpression = leftValue
		} else {
			return nil, nil, fmt.Errorf("Invalid time condition %v", condition)
		}

		switch expr.Name {
		case ">":
			if isParsingStartTime && !isTimeOnLeft || !isParsingStartTime && !isTimeOnRight {
				return condition, nil, nil
			}
		case "<":
			if !isParsingStartTime && !isTimeOnLeft || isParsingStartTime && !isTimeOnRight {
				return condition, nil, nil
			}
		case "=":
			nanoseconds, err := parseTime(timeExpression)
			if err != nil {
				return nil, nil, err
			}
			t := time.Unix(nanoseconds/int64(time.Second), nanoseconds%int64(time.Second)).UTC()
			return condition, &t, nil
		default:
			return nil, nil, fmt.Errorf("Cannot use time with '%s'", expr.Name)
		}

		nanoseconds, err := parseTime(timeExpression)
		if err != nil {
			return nil, nil, err
		}
		t := time.Unix(nanoseconds/int64(time.Second), nanoseconds%int64(time.Second)).UTC()
		return nil, &t, nil
	}

	leftCondition, _ := condition.GetLeftWhereCondition()
	newLeftCondition, timeLeft, err := getTime(leftCondition, isParsingStartTime)
	if err != nil {
		return nil, nil, err
	}
	newRightCondition, timeRight, err := getTime(condition.Right, isParsingStartTime)
	if err != nil {
		return nil, nil, err
	}

	if condition.Operation == "OR" && (timeLeft != nil || timeRight != nil) {
		// we can't have two start times or'd together
		return nil, nil, fmt.Errorf("Invalid where clause, time must appear twice to specify start and end time")
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

	if timeLeft == nil {
		return newCondition, timeRight, nil
	}
	if timeRight == nil {
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
