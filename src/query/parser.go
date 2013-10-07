package query

// #include "query_types.h"
// #include <stdlib.h>
import "C"

import (
	"errors"
	"unsafe"
)

type From struct {
	TableName string
}

type Operation int

type Value struct {
	Name  string
	Elems []string
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

type WhereCondition struct {
	Left      *BoolExpression
	Operation string
	Right     *BoolExpression
}

type Query struct {
	q           C.query
	ColumnNames []string
	Condition   *WhereCondition
}

func (self *Query) GetColumnNames() []string {
	if self.ColumnNames != nil {
		return self.ColumnNames
	}

	self.ColumnNames = GetStringArray(self.q.c)
	return self.ColumnNames
}

func (self *Query) GetFromClause() *From {
	return &From{C.GoString(self.q.f.table)}
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
	v.Elems = GetStringArray(value.args)

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

func (self *Query) Close() {
	C.close_query(&self.q)
}

func ParseQuery(query string) (*Query, error) {
	queryString := C.CString(query)
	defer C.free(unsafe.Pointer(queryString))
	q := C.parse_query(queryString)
	var err error
	if q.error != nil {
		err = errors.New(C.GoString(q.error))
	}
	return &Query{q, nil, nil}, err
}
