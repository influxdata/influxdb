package parser

// #include "query_types.h"
// #include <stdlib.h>
import "C"

import (
	"common"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"strconv"
	"time"
	"unsafe"
)

type From struct {
	TableName string
}

type Operation int

type ValueType int

const (
	ValueRegex        ValueType = C.VALUE_REGEX
	ValueInt                    = C.VALUE_INT
	ValueFloat                  = C.VALUE_FLOAT
	ValueString                 = C.VALUE_STRING
	ValueTableName              = C.VALUE_TABLE_NAME
	ValueSimpleName             = C.VALUE_SIMPLE_NAME
	ValueDuration               = C.VALUE_DURATION
	ValueWildcard               = C.VALUE_WILDCARD
	ValueFunctionCall           = C.VALUE_FUNCTION_CALL
	ValueExpression             = C.VALUE_EXPRESSION
)

type Value struct {
	Name          string
	Type          ValueType
	Elems         []*Value
	compiledRegex *regexp.Regexp
}

func (self *Value) IsFunctionCall() bool {
	return self.Type == ValueFunctionCall
}

func (self *Value) GetCompiledRegex() (*regexp.Regexp, bool) {
	return self.compiledRegex, self.Type == ValueRegex
}

type FromClauseType int

const (
	FromClauseArray     FromClauseType = C.FROM_ARRAY
	FromClauseMerge     FromClauseType = C.FROM_MERGE
	FromClauseInnerJoin FromClauseType = C.FROM_INNER_JOIN
)

type TableName struct {
	Name  *Value
	Alias string
}

func (self *TableName) GetAlias() string {
	if self.Alias != "" {
		return self.Alias
	}
	return self.Name.Name
}

type FromClause struct {
	Type  FromClauseType
	Names []*TableName
}

type IntoClause struct {
	Target *Value
}

type GroupByClause struct {
	FillWithZero bool
	FillValue    *Value
	Elems        []*Value
}

func (self GroupByClause) GetGroupByTime() (*time.Duration, error) {
	for _, groupBy := range self.Elems {
		if groupBy.IsFunctionCall() {
			// TODO: check the number of arguments and return an error
			if len(groupBy.Elems) != 1 {
				return nil, common.NewQueryError(common.WrongNumberOfArguments, "time function only accepts one argument")
			}
			// TODO: check the function name
			// TODO: error checking
			arg := groupBy.Elems[0].Name
			duration, err := time.ParseDuration(arg)
			if err != nil {
				return nil, common.NewQueryError(common.InvalidArgument, fmt.Sprintf("invalid argument %s to the time function", arg))
			}
			return &duration, nil
		}
	}
	return nil, nil
}

type WhereCondition struct {
	isBooleanExpression bool
	Left                interface{}
	Operation           string
	Right               *WhereCondition
}

func (self *WhereCondition) GetBoolExpression() (*Value, bool) {
	if self.isBooleanExpression {
		return self.Left.(*Value), true
	}
	return nil, false
}

func (self *WhereCondition) GetLeftWhereCondition() (*WhereCondition, bool) {
	if !self.isBooleanExpression {
		return self.Left.(*WhereCondition), true
	}
	return nil, false
}

type BasicQuery struct {
	queryString string
	startTime   time.Time
	endTime     time.Time
}

type SelectDeleteCommonQuery struct {
	BasicQuery
	FromClause *FromClause
	Condition  *WhereCondition
	endTimeSet bool
}

type SelectQuery struct {
	SelectDeleteCommonQuery
	ColumnNames   []*Value
	groupByClause *GroupByClause
	IntoClause    *IntoClause
	Limit         int
	Ascending     bool
}

type ListType int

const (
	Series ListType = iota
	ContinuousQueries
)

type ListQuery struct {
	Type ListType
}

type DropQuery struct {
	Id int
}

type DropSeriesQuery struct {
	tableName string
}

func (self *DropSeriesQuery) GetTableName() string {
	return self.tableName
}

type DeleteQuery struct {
	SelectDeleteCommonQuery
}

type Query struct {
	SelectQuery     *SelectQuery
	DeleteQuery     *DeleteQuery
	ListQuery       *ListQuery
	DropSeriesQuery *DropSeriesQuery
	DropQuery       *DropQuery
}

func (self *Query) GetQueryString() string {
	if self.SelectQuery != nil {
		return self.SelectQuery.GetQueryString()
	}
	return self.DeleteQuery.GetQueryString()
}

func (self *Query) IsListQuery() bool {
	return self.ListQuery != nil
}

func (self *Query) IsListSeriesQuery() bool {
	return self.ListQuery != nil && self.ListQuery.Type == Series
}

func (self *Query) IsListContinuousQueriesQuery() bool {
	return self.ListQuery != nil && self.ListQuery.Type == ContinuousQueries
}

func (self *BasicQuery) GetQueryString() string {
	return self.queryString
}

func (self *SelectQuery) GetColumnNames() []*Value {
	return self.ColumnNames
}

func (self *SelectQuery) IsSinglePointQuery() bool {
	w := self.GetWhereCondition()
	if w == nil {
		return false
	}

	leftWhereCondition, ok := w.GetLeftWhereCondition()
	if !ok {
		return false
	}

	leftBoolExpression, ok := leftWhereCondition.GetBoolExpression()
	if !ok {
		return false
	}

	rightBoolExpression, ok := w.Right.GetBoolExpression()
	if !ok {
		return false
	}

	if leftBoolExpression.Name != "=" && rightBoolExpression.Name != "=" {
		return false
	}

	if leftBoolExpression.Elems[0].Name != "time" || rightBoolExpression.Elems[0].Name != "sequence_number" {
		return false
	}

	return true
}

func (self *SelectQuery) GetSinglePointQuerySequenceNumber() (int64, error) {
	w := self.GetWhereCondition()
	rightBoolExpression, _ := w.Right.GetBoolExpression()
	sequence := rightBoolExpression.Elems[1].Name
	sequence_number, err := strconv.ParseInt(sequence, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("The column sequence_number can only be queried as an integer.")
	}
	return sequence_number, nil
}

func (self *SelectQuery) IsContinuousQuery() bool {
	return self.GetIntoClause() != nil
}

func (self *SelectQuery) GetIntoClause() *IntoClause {
	return self.IntoClause
}

func (self *SelectDeleteCommonQuery) GetFromClause() *FromClause {
	return self.FromClause
}

func setupSlice(hdr *reflect.SliceHeader, ptr unsafe.Pointer, size C.size_t) {
	hdr.Cap = int(size)
	hdr.Len = int(size)
	hdr.Data = uintptr(ptr)
}

func GetGroupByClause(groupByClause *C.groupby_clause) (*GroupByClause, error) {
	if groupByClause == nil {
		return &GroupByClause{Elems: nil}, nil
	}

	values, err := GetValueArray(groupByClause.elems)
	if err != nil {
		return nil, err
	}

	fillWithZero := false
	var fillValue *Value

	if groupByClause.fill_function != nil {
		fun, err := GetValue(groupByClause.fill_function)
		if err != nil {
			return nil, err
		}
		if fun.Name != "fill" {
			return nil, fmt.Errorf("You can't use %s with group by", fun.Name)
		}

		if len(fun.Elems) != 1 {
			return nil, fmt.Errorf("`fill` accepts one argument only")
		}

		fillValue = fun.Elems[0]
		fillWithZero = true
	}

	return &GroupByClause{
		Elems:        values,
		FillWithZero: fillWithZero,
		FillValue:    fillValue,
	}, nil
}

func GetValueArray(array *C.value_array) ([]*Value, error) {
	if array == nil {
		return nil, nil
	}

	var values []*C.value
	setupSlice((*reflect.SliceHeader)((unsafe.Pointer(&values))), unsafe.Pointer(array.elems), array.size)

	valuesSlice := make([]*Value, 0, array.size)

	for _, value := range values {
		value, err := GetValue(value)
		if err != nil {
			return nil, err
		}
		valuesSlice = append(valuesSlice, value)
	}
	return valuesSlice, nil
}

func GetStringArray(array *C.array) []string {
	if array == nil {
		return nil
	}

	var values []*C.char
	setupSlice((*reflect.SliceHeader)((unsafe.Pointer(&values))), unsafe.Pointer(array.elems), array.size)

	stringSlice := make([]string, 0, array.size)

	for _, value := range values {
		stringSlice = append(stringSlice, C.GoString(value))
	}
	return stringSlice
}

func GetValue(value *C.value) (*Value, error) {
	v := &Value{}
	v.Name = C.GoString(value.name)
	var err error
	v.Elems, err = GetValueArray(value.args)
	if err != nil {
		return nil, err
	}
	v.Type = ValueType(value.value_type)
	isCaseInsensitive := value.is_case_insensitive != 0
	if v.Type == ValueRegex {
		if isCaseInsensitive {
			v.compiledRegex, err = regexp.Compile("(?i)" + v.Name)
		} else {
			v.compiledRegex, err = regexp.Compile(v.Name)
		}
	}
	return v, err
}

func GetTableName(name *C.table_name) (*TableName, error) {
	value, err := GetValue(name.name)
	if err != nil {
		return nil, err
	}

	table := &TableName{Name: value}
	if name.alias != nil {
		table.Alias = C.GoString(name.alias)
	}

	return table, nil
}

func GetTableNameArray(array *C.table_name_array) ([]*TableName, error) {
	var names []*C.table_name
	setupSlice((*reflect.SliceHeader)((unsafe.Pointer(&names))), unsafe.Pointer(array.elems), array.size)

	tableNamesSlice := make([]*TableName, 0, array.size)
	for _, name := range names {
		tableName, err := GetTableName(name)
		if err != nil {
			return nil, err
		}
		tableNamesSlice = append(tableNamesSlice, tableName)
	}
	return tableNamesSlice, nil
}

func GetFromClause(fromClause *C.from_clause) (*FromClause, error) {
	arr, err := GetTableNameArray(fromClause.names)
	if err != nil {
		return nil, err
	}
	return &FromClause{FromClauseType(fromClause.from_clause_type), arr}, nil
}

func GetIntoClause(intoClause *C.into_clause) (*IntoClause, error) {
	if intoClause == nil {
		return nil, nil
	}

	target, err := GetValue(intoClause.target)
	if err != nil {
		return nil, err
	}

	return &IntoClause{target}, nil
}

func GetWhereCondition(condition *C.condition) (*WhereCondition, error) {
	if condition.is_bool_expression != 0 {
		expr, err := GetValue((*C.value)(condition.left))
		if err != nil {
			return nil, err
		}
		return &WhereCondition{
			isBooleanExpression: true,
			Left:                expr,
			Operation:           "",
			Right:               nil,
		}, nil
	}

	c := &WhereCondition{}
	var err error
	c.Left, err = GetWhereCondition((*C.condition)(condition.left))
	if err != nil {
		return nil, err
	}
	c.Operation = C.GoString(condition.op)
	c.Right, err = GetWhereCondition((*C.condition)(unsafe.Pointer(condition.right)))

	return c, err
}

func (self *SelectDeleteCommonQuery) GetWhereCondition() *WhereCondition {
	return self.Condition
}

func (self *SelectQuery) GetGroupByClause() *GroupByClause {
	return self.groupByClause
}

// This is just for backward compatability so we don't have
// to change all the code.
func ParseSelectQuery(query string) (*SelectQuery, error) {
	queries, err := ParseQuery(query)
	if err != nil {
		return nil, err
	}

	if len(queries) == 0 {
		return nil, fmt.Errorf("No queries found")
	}

	selectQuery := queries[0].SelectQuery
	if selectQuery == nil {
		return nil, fmt.Errorf("Query isn't a select query: '%s'", queries[0].GetQueryString())
	}

	return selectQuery, nil
}

func ParseQuery(query string) ([]*Query, error) {
	queryString := C.CString(query)
	defer C.free(unsafe.Pointer(queryString))
	q := C.parse_query(queryString)
	defer C.close_query(&q)

	if q.error != nil {
		str := C.GoString(q.error.err)
		err := fmt.Errorf("Error at %d:%d. %s", q.error.line, q.error.column, str)
		return nil, err
	}

	if q.list_series_query != 0 {
		return []*Query{&Query{ListQuery: &ListQuery{Type: Series}}}, nil
	}

	if q.list_continuous_queries_query != 0 {
		return []*Query{&Query{ListQuery: &ListQuery{Type: ContinuousQueries}}}, nil
	}

	if q.select_query != nil {
		selectQuery, err := parseSelectQuery(query, q.select_query)
		if err != nil {
			return nil, err
		}

		return []*Query{&Query{SelectQuery: selectQuery}}, nil
	} else if q.delete_query != nil {
		deleteQuery, err := parseDeleteQuery(query, q.delete_query)
		if err != nil {
			return nil, err
		}
		return []*Query{&Query{DeleteQuery: deleteQuery}}, nil
	} else if q.drop_series_query != nil {
		dropSeriesQuery, err := parseDropSeriesQuery(query, q.drop_series_query)
		if err != nil {
			return nil, err
		}
		return []*Query{&Query{DropSeriesQuery: dropSeriesQuery}}, nil
	} else if q.drop_query != nil {
		fmt.Println(q.drop_query.id)
		return []*Query{&Query{DropQuery: &DropQuery{Id: int(q.drop_query.id)}}}, nil
	}
	return nil, fmt.Errorf("Unknown query type encountered")
}

func parseDropSeriesQuery(queryStirng string, dropSeriesQuery *C.drop_series_query) (*DropSeriesQuery, error) {
	name, err := GetValue(dropSeriesQuery.name)
	if err != nil {
		return nil, err
	}

	return &DropSeriesQuery{
		tableName: name.Name,
	}, nil
}

func parseSelectDeleteCommonQuery(queryString string, fromClause *C.from_clause, whereCondition *C.condition) (SelectDeleteCommonQuery, error) {

	goQuery := SelectDeleteCommonQuery{
		BasicQuery: BasicQuery{
			queryString: queryString,
			startTime:   time.Unix(0, 0),
			endTime:     time.Now(),
		},
	}

	var err error

	// get the from clause
	goQuery.FromClause, err = GetFromClause(fromClause)
	if err != nil {
		return goQuery, err
	}

	// get the where condition
	if whereCondition != nil {
		goQuery.Condition, err = GetWhereCondition(whereCondition)
		if err != nil {
			return goQuery, err
		}
	}

	var startTime, endTime time.Time

	goQuery.Condition, endTime, err = getTime(goQuery.GetWhereCondition(), false)
	if err != nil {
		return goQuery, err
	}

	goQuery.endTimeSet = endTime.Unix() > 0

	if goQuery.endTimeSet {
		goQuery.endTime = endTime
	}

	goQuery.Condition, startTime, err = getTime(goQuery.GetWhereCondition(), true)
	if err != nil {
		return goQuery, err
	}

	if startTime.Unix() > 0 {
		goQuery.startTime = startTime
	} else if goQuery.endTime.Unix() > 0 {
		goQuery.startTime = time.Unix(math.MinInt64, 0)
	}

	return goQuery, nil
}

func parseSelectQuery(queryString string, q *C.select_query) (*SelectQuery, error) {
	limit := q.limit
	if limit == -1 {
		// no limit by default
		limit = 0
	}

	basicQuery, err := parseSelectDeleteCommonQuery(queryString, q.from_clause, q.where_condition)
	if err != nil {
		return nil, err
	}

	goQuery := &SelectQuery{
		SelectDeleteCommonQuery: basicQuery,
		Limit:     int(limit),
		Ascending: q.ascending != 0,
	}

	// get the column names
	goQuery.ColumnNames, err = GetValueArray(q.c)
	if err != nil {
		return nil, err
	}

	// get the group by clause
	if q.group_by == nil {
		goQuery.groupByClause = &GroupByClause{}
	} else {
		goQuery.groupByClause, err = GetGroupByClause(q.group_by)
		if err != nil {
			return nil, err
		}
	}

	// get the into clause
	goQuery.IntoClause, err = GetIntoClause(q.into_clause)
	if err != nil {
		return goQuery, err
	}

	return goQuery, nil
}

func parseDeleteQuery(queryString string, query *C.delete_query) (*DeleteQuery, error) {
	basicQuery, err := parseSelectDeleteCommonQuery(queryString, query.from_clause, query.where_condition)
	if err != nil {
		return nil, err
	}
	goQuery := &DeleteQuery{
		SelectDeleteCommonQuery: basicQuery,
	}
	if basicQuery.GetWhereCondition() != nil {
		return nil, fmt.Errorf("Delete queries can't have where clause that don't reference time")
	}
	return goQuery, nil
}
