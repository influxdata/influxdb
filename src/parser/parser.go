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
	"time"
	"unsafe"
)

type From struct {
	TableName string
}

type Operation int

type ValueType int

const (
	DEFAULT_LIMIT = 10000
)

const (
	ValueRegex        ValueType = C.VALUE_REGEX
	ValueInt          ValueType = C.VALUE_INT
	ValueFloat        ValueType = C.VALUE_FLOAT
	ValueString       ValueType = C.VALUE_STRING
	ValueTableName    ValueType = C.VALUE_TABLE_NAME
	ValueSimpleName   ValueType = C.VALUE_SIMPLE_NAME
	ValueDuration     ValueType = C.VALUE_DURATION
	ValueWildcard     ValueType = C.VALUE_WILDCARD
	ValueFunctionCall ValueType = C.VALUE_FUNCTION_CALL
)

type Value struct {
	Name              string
	Type              ValueType
	IsCaseInsensitive bool
	Elems             []*Value
	compiledRegex     *regexp.Regexp
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

type Expression struct {
	Left      interface{}
	Operation byte
	Right     *Expression
}

type BoolExpression struct {
	Left      *Expression
	Operation string
	Right     *Expression
}

type GroupByClause []*Value

func (self GroupByClause) GetGroupByTime() (*time.Duration, error) {
	for _, groupBy := range self {
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

func (self *WhereCondition) GetBoolExpression() (*BoolExpression, bool) {
	if self.isBooleanExpression {
		return self.Left.(*BoolExpression), true
	}
	return nil, false
}

func (self *WhereCondition) GetLeftWhereCondition() (*WhereCondition, bool) {
	if !self.isBooleanExpression {
		return self.Left.(*WhereCondition), true
	}
	return nil, false
}

type Query struct {
	queryString   string
	ColumnNames   []*Value
	FromClause    *FromClause
	Condition     *WhereCondition
	groupByClause GroupByClause
	Limit         int
	Ascending     bool
	startTime     time.Time
	endTime       time.Time
}

func (self *Query) GetQueryString() string {
	return self.queryString
}

func (self *Query) GetColumnNames() []*Value {
	return self.ColumnNames
}

func (self *Query) GetFromClause() *FromClause {
	return self.FromClause
}

func (self *Expression) GetLeftValue() (*Value, bool) {
	if self.Operation == 0 {
		return self.Left.(*Value), true
	}
	return nil, false
}

func (self *Expression) GetLeftExpression() (*Expression, bool) {
	if self.Operation != 0 {
		return self.Left.(*Expression), true
	}
	return nil, false
}

func setupSlice(hdr *reflect.SliceHeader, ptr unsafe.Pointer, size C.size_t) {
	hdr.Cap = int(size)
	hdr.Len = int(size)
	hdr.Data = uintptr(ptr)
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
	v.IsCaseInsensitive = value.is_case_insensitive != 0
	if v.Type == ValueRegex {
		v.compiledRegex, err = regexp.Compile(v.Name)
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

func GetExpression(expr *C.expression) (*Expression, error) {
	expression := &Expression{}
	if expr.op == 0 {
		value, err := GetValue((*C.value)(expr.left))
		if err != nil {
			return nil, err
		}
		expression.Left = value
		expression.Operation = byte(expr.op)
		expression.Right = nil
	} else {
		var err error
		expression.Left, err = GetExpression((*C.expression)(expr.left))
		if err != nil {
			return nil, err
		}
		expression.Operation = byte(expr.op)
		expression.Right, err = GetExpression((*C.expression)(unsafe.Pointer(expr.right)))
		if err != nil {
			return nil, err
		}
	}

	return expression, nil
}

func GetBoolExpression(expr *C.bool_expression) (*BoolExpression, error) {
	boolExpression := &BoolExpression{}
	var err error
	boolExpression.Left, err = GetExpression(expr.left)
	if err != nil {
		return nil, err
	}
	if expr.op != nil {
		boolExpression.Operation = C.GoString(expr.op)
		boolExpression.Right, err = GetExpression(expr.right)
	}

	return boolExpression, err
}

func GetWhereCondition(condition *C.condition) (*WhereCondition, error) {
	if condition.is_bool_expression != 0 {
		expr, err := GetBoolExpression((*C.bool_expression)(condition.left))
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

func (self *Query) GetWhereCondition() *WhereCondition {
	return self.Condition
}

func (self *Query) GetGroupByClause() GroupByClause {
	if self.groupByClause != nil {
		return self.groupByClause
	}

	return self.groupByClause
}

func ParseQuery(query string) (*Query, error) {
	queryString := C.CString(query)
	defer C.free(unsafe.Pointer(queryString))
	q := C.parse_query(queryString)
	defer C.close_query(&q)

	if q.error != nil {
		str := C.GoString(q.error.err)
		err := fmt.Errorf("Error at %d:%d. %s", q.error.line, q.error.column, str)
		return nil, err
	}

	limit := q.limit
	if limit == -1 {
		limit = DEFAULT_LIMIT
	}

	goQuery := &Query{query, nil, nil, nil, nil, int(limit), q.ascending != 0, time.Unix(0, 0), time.Now()}

	var err error

	// get the column names
	goQuery.ColumnNames, err = GetValueArray(q.c)
	if err != nil {
		return nil, err
	}

	// get the from clause
	goQuery.FromClause, err = GetFromClause(q.from_clause)
	if err != nil {
		return nil, err
	}

	// get the where condition
	if q.where_condition != nil {
		goQuery.Condition, err = GetWhereCondition(q.where_condition)
		if err != nil {
			return nil, err
		}
	}

	// get the group by clause
	if q.group_by == nil {
		goQuery.groupByClause = GroupByClause{}
	} else {
		goQuery.groupByClause, err = GetValueArray(q.group_by)
		if err != nil {
			return nil, err
		}
	}

	var startTime, endTime time.Time

	goQuery.Condition, endTime, err = getTime(goQuery.GetWhereCondition(), false)
	if err != nil {
		return nil, err
	}

	if endTime.Unix() > 0 {
		goQuery.endTime = endTime
	}

	goQuery.Condition, startTime, err = getTime(goQuery.GetWhereCondition(), true)
	if err != nil {
		return nil, err
	}

	if startTime.Unix() > 0 {
		goQuery.startTime = startTime
	} else if goQuery.endTime.Unix() > 0 {
		goQuery.startTime = time.Unix(math.MinInt64, 0)
	}

	return goQuery, nil
}
