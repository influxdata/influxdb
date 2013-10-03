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
	EQUAL Operation = C.OP_EQUAL
)

type WhereClause struct {
	ColumnName string
	Op         Operation
	Value      interface{}
}

type Query struct {
	q C.query
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
	return &Query{q}, err
}
