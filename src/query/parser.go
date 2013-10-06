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

const (
	EQUAL               Operation = C.OP_EQ
	NOT_EQUAL                     = C.OP_NE
	GREATER_THAN                  = C.OP_GT
	LESS_THAN                     = C.OP_LT
	GREATHER_THAN_OR_EQ           = C.OP_GE
	LESS_THAN_OR_EQ               = C.OP_LE
)

type WhereClause struct {
	ColumnName string
	Op         Operation
	Value      interface{}
}

type Query struct {
	q           C.query
	ColumnNames []string
}

func (self *Query) GetColumnNames() []string {
	if self.ColumnNames != nil {
		return self.ColumnNames
	}

	arr := uintptr(unsafe.Pointer(self.q.c.elems))
	elemSize := unsafe.Sizeof(*self.q.c.elems)
	size := uintptr(self.q.c.size)

	var i uintptr
	for i = 0; i < size; i++ {
		str := (**C.char)(unsafe.Pointer(arr + elemSize*i))
		self.ColumnNames = append(self.ColumnNames, C.GoString(*str))
	}
	return self.ColumnNames
}

func (self *Query) GetFromClause() *From {
	return &From{C.GoString(self.q.f.table)}
}

func (self *Query) GetWhereClause() *WhereClause {
	return &WhereClause{
		ColumnName: C.GoString(self.q.w.column_name),
		Op:         Operation(self.q.w.op),
		Value:      C.GoString(self.q.w.v.svalue),
	}
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
	return &Query{q, nil}, err
}
