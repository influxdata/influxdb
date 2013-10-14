package parser

// #include "query_types.h"
// #include <stdlib.h>
import "C"

import (
	"common"
	"fmt"
	"reflect"
	"time"
	"unsafe"
)

type From struct {
	TableName string
}

type Operation int

type Value struct {
	Name  string
	Elems []*Value
}

func (self *Value) IsFunctionCall() bool {
	return self.Elems != nil
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
	q             C.query
	closed        bool
	ColumnNames   []*Value
	Condition     *WhereCondition
	groupByClause GroupByClause
	Limit         int
	Ascending     bool
}

func (self *Query) GetColumnNames() []*Value {
	if self.ColumnNames != nil {
		return self.ColumnNames
	}

	self.ColumnNames = GetValueArray(self.q.c)
	return self.ColumnNames
}

func (self *Query) GetFromClause() *Value {
	return GetValue(self.q.f)
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

func GetValueArray(array *C.value_array) []*Value {
	if array == nil {
		return nil
	}

	var values []*C.value
	setupSlice((*reflect.SliceHeader)((unsafe.Pointer(&values))), unsafe.Pointer(array.elems), array.size)

	valuesSlice := make([]*Value, 0, array.size)

	for _, value := range values {
		valuesSlice = append(valuesSlice, GetValue(value))
	}
	return valuesSlice
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

func GetValue(value *C.value) *Value {
	v := &Value{}
	v.Name = C.GoString(value.name)
	v.Elems = GetValueArray(value.args)

	return v
}

func GetExpression(expr *C.expression) *Expression {
	expression := &Expression{}
	if expr.op == 0 {
		expression.Left = GetValue((*C.value)(expr.left))
		expression.Operation = byte(expr.op)
		expression.Right = nil
	} else {
		expression.Left = GetExpression((*C.expression)(expr.left))
		expression.Operation = byte(expr.op)
		expression.Right = GetExpression((*C.expression)(unsafe.Pointer(expr.right)))
	}

	return expression
}

func GetBoolExpression(expr *C.bool_expression) *BoolExpression {
	boolExpression := &BoolExpression{}
	boolExpression.Left = GetExpression(expr.left)
	if expr.op != nil {
		boolExpression.Operation = C.GoString(expr.op)
		boolExpression.Right = GetExpression(expr.right)
	}

	return boolExpression
}

func GetWhereCondition(condition *C.condition) *WhereCondition {
	if condition.is_bool_expression != 0 {
		return &WhereCondition{
			isBooleanExpression: true,
			Left:                GetBoolExpression((*C.bool_expression)(condition.left)),
			Operation:           "",
			Right:               nil,
		}
	}

	c := &WhereCondition{}
	c.Left = GetWhereCondition((*C.condition)(condition.left))
	c.Operation = C.GoString(condition.op)
	c.Right = GetWhereCondition((*C.condition)(unsafe.Pointer(condition.right)))

	return c
}

func (self *Query) GetWhereCondition() *WhereCondition {
	if self.q.where_condition == nil {
		return nil
	}

	self.Condition = GetWhereCondition(self.q.where_condition)
	return self.Condition
}

func (self *Query) GetGroupByClause() GroupByClause {
	if self.groupByClause != nil {
		return self.groupByClause
	}

	if self.q.group_by == nil {
		self.groupByClause = GroupByClause{}
		return self.groupByClause
	}

	self.groupByClause = GetValueArray(self.q.group_by)
	return self.groupByClause
}

func (self *Query) Close() {
	if self.closed {
		return
	}

	C.close_query(&self.q)
	self.closed = true
}

func ParseQuery(query string) (*Query, error) {
	queryString := C.CString(query)
	defer C.free(unsafe.Pointer(queryString))
	q := C.parse_query(queryString)
	var err error
	if q.error != nil {
		str := C.GoString(q.error.err)
		err = fmt.Errorf("Error at %d:%d. %s", q.error.line, q.error.column, str)
		C.close_query(&q)
		return nil, err
	}
	return &Query{q, false, nil, nil, nil, int(q.limit), q.ascending != 0}, err
}
