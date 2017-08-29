package storage

import "github.com/influxdata/influxdb/influxql"

// TODO(sgc): build expression evaluator that does not use influxql AST

type expression interface {
	EvalBool(v valuer) bool
}

type expressionContext interface {
	Int64(slot int) int64
}

type astExpr struct {
	expr influxql.Expr
}

type int64Slot struct {
	val int64
}

func (e *astExpr) EvalBool(v valuer) bool {
	return evalExprBool(e.expr, v)
}

// valuer is the interface that wraps the Value() method.
type valuer interface {
	// Value returns the value and existence flag for a given key.
	Value(key string) (interface{}, bool)
}
