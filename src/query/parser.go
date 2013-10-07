package query

// #include "query_types.h"
// #include <stdlib.h>
import "C"

import (
	"fmt"
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
	Left      *Value
	Operation byte
	Right     *Value
}

type BoolExpression struct {
	Left      *Expression
	Operation string
	Right     *Expression
}

type GroupByClause []*Value

type WhereCondition struct {
	Left      *BoolExpression
	Operation string
	Right     *BoolExpression
}

type Query struct {
	q             C.query
	ColumnNames   []*Value
	Condition     *WhereCondition
	groupByClause GroupByClause
}

func (self *Query) GetColumnNames() []*Value {
	if self.ColumnNames != nil {
		return self.ColumnNames
	}

	self.ColumnNames = GetValueArray(self.q.c)
	return self.ColumnNames
}

func (self *Query) GetFromClause() *From {
	return &From{C.GoString(self.q.f.table)}
}

func GetValueArray(array *C.value_array) []*Value {
	if array == nil {
		return nil
	}

	arr := uintptr(unsafe.Pointer(array.elems))
	elemSize := unsafe.Sizeof(*array.elems)
	size := uintptr(array.size)

	stringSlice := make([]*Value, 0, size)

	var i uintptr
	for i = 0; i < size; i++ {
		str := (**C.value)(unsafe.Pointer(arr + elemSize*i))
		stringSlice = append(stringSlice, GetValue(*str))
	}
	return stringSlice
}

func GetStringArray(array *C.array) []string {
	if array == nil {
		return nil
	}

	arr := uintptr(unsafe.Pointer(array.elems))
	elemSize := unsafe.Sizeof(*array.elems)
	size := uintptr(array.size)

	stringSlice := make([]string, 0, size)

	var i uintptr
	for i = 0; i < size; i++ {
		str := (**C.char)(unsafe.Pointer(arr + elemSize*i))
		stringSlice = append(stringSlice, C.GoString(*str))
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
	expression.Left = GetValue(expr.left)
	if expr.op != '\000' {
		expression.Operation = byte(expr.op)
		expression.Right = GetValue(expr.right)
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

func (self *Query) GetWhereCondition() *WhereCondition {
	if self.q.where_condition == nil {
		return nil
	}

	condition := &WhereCondition{}
	condition.Left = GetBoolExpression(self.q.where_condition.left)
	if self.q.where_condition.op != nil {
		condition.Operation = C.GoString(self.q.where_condition.op)
		condition.Right = GetBoolExpression(self.q.where_condition.right)
	}

	return condition
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
	C.close_query(&self.q)
}

func ParseQuery(query string) (*Query, error) {
	queryString := C.CString(query)
	defer C.free(unsafe.Pointer(queryString))
	q := C.parse_query(queryString)
	var err error
	if q.error != nil {
		str := C.GoString(q.error.err)
		err = fmt.Errorf("Error at %d:%d. %s", q.error.line, q.error.column, str)
	}
	return &Query{q, nil, nil, nil}, err
}
